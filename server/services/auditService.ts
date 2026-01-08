import { db } from '../db';
import { 
  auditLogs, 
  auditLogEntries, 
  InsertAuditLog, 
  InsertAuditLogEntry,
  AuditEventType,
  AuditEventData
} from '@shared/schema';
import { eq, desc, asc } from 'drizzle-orm';
import { WebSocket, WebSocketServer } from 'ws';
import { Server } from 'http';

type AuditCallback = (entry: AuditLogEntry) => void;

interface AuditLogEntry {
  sequenceNum: number;
  timestamp: Date;
  eventType: AuditEventType;
  eventData: any;
}

interface ActiveAudit {
  auditLogId: number;
  userId: number;
  jobType: string;
  jobId: number | null;
  sequenceCounter: number;
  subscribers: Set<WebSocket>;
  entries: AuditLogEntry[];
}

const activeAudits = new Map<number, ActiveAudit>();
const clientConnections = new Map<WebSocket, number | null>();

let wss: WebSocketServer | null = null;

export function initAuditWebSocket(server: Server): void {
  wss = new WebSocketServer({ server, path: '/ws/audit' });
  
  wss.on('connection', (ws: WebSocket) => {
    console.log('[AUDIT-WS] New audit WebSocket connection');
    clientConnections.set(ws, null);
    
    ws.on('message', async (data) => {
      try {
        const message = JSON.parse(data.toString());
        
        if (message.type === 'subscribe' && message.auditLogId) {
          const auditLogId = message.auditLogId;
          clientConnections.set(ws, auditLogId);
          
          const audit = activeAudits.get(auditLogId);
          if (audit) {
            audit.subscribers.add(ws);
            for (const entry of audit.entries) {
              ws.send(JSON.stringify({ type: 'entry', auditLogId, entry }));
            }
          }
          
          ws.send(JSON.stringify({ type: 'subscribed', auditLogId }));
        }
        
        if (message.type === 'get_history' && message.auditLogId) {
          const entries = await getAuditEntries(message.auditLogId);
          ws.send(JSON.stringify({ type: 'history', auditLogId: message.auditLogId, entries }));
        }
      } catch (error) {
        console.error('[AUDIT-WS] Error processing message:', error);
      }
    });
    
    ws.on('close', () => {
      const auditLogId = clientConnections.get(ws);
      if (auditLogId !== null && auditLogId !== undefined) {
        const audit = activeAudits.get(auditLogId);
        if (audit) {
          audit.subscribers.delete(ws);
        }
      }
      clientConnections.delete(ws);
    });
  });
  
  console.log('[AUDIT-WS] WebSocket server initialized on /ws/audit');
}

export async function startAudit(
  userId: number,
  jobType: string,
  jobId?: number
): Promise<number> {
  const startTime = Date.now();
  
  const insertData: InsertAuditLog = {
    userId,
    jobType,
    jobId: jobId ?? null,
    startedAt: new Date(),
    status: 'running'
  };
  
  const [result] = await db.insert(auditLogs).values(insertData).returning({ id: auditLogs.id });
  const auditLogId = result.id;
  
  const audit: ActiveAudit = {
    auditLogId,
    userId,
    jobType,
    jobId: jobId ?? null,
    sequenceCounter: 0,
    subscribers: new Set(),
    entries: []
  };
  
  activeAudits.set(auditLogId, audit);
  
  await logEvent(auditLogId, 'job_started', {
    jobType,
    targetWords: undefined,
    inputWords: undefined
  } as AuditEventData['job_started']);
  
  console.log(`[AUDIT] Started audit log ${auditLogId} for job type: ${jobType}`);
  
  return auditLogId;
}

export async function logEvent<T extends AuditEventType>(
  auditLogId: number,
  eventType: T,
  eventData: AuditEventData[T]
): Promise<void> {
  const audit = activeAudits.get(auditLogId);
  const sequenceNum = audit ? ++audit.sequenceCounter : 1;
  const timestamp = new Date();
  
  const entry: AuditLogEntry = {
    sequenceNum,
    timestamp,
    eventType,
    eventData
  };
  
  const insertData: InsertAuditLogEntry = {
    auditLogId,
    sequenceNum,
    timestamp,
    eventType,
    eventData: eventData as any
  };
  
  try {
    await db.insert(auditLogEntries).values(insertData);
  } catch (error) {
    console.error(`[AUDIT] Failed to write audit entry:`, error);
  }
  
  if (audit) {
    audit.entries.push(entry);
    broadcastEntry(auditLogId, entry);
  }
}

export async function logDbQuery(
  auditLogId: number,
  sql: string,
  table: string,
  operation: 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE',
  rowsReturned: number,
  durationMs: number,
  params?: any[]
): Promise<void> {
  await logEvent(auditLogId, 'db_query', {
    sql: sql.substring(0, 500),
    params,
    table,
    operation,
    rowsReturned,
    durationMs
  });
}

export async function logDbInsert(
  auditLogId: number,
  table: string,
  rowId: number,
  keyFields: Record<string, any>,
  durationMs: number
): Promise<void> {
  await logEvent(auditLogId, 'db_insert', {
    table,
    rowId,
    keyFields,
    durationMs
  });
}

export async function logDbUpdate(
  auditLogId: number,
  table: string,
  rowId: number,
  fieldsUpdated: string[],
  newValues: Record<string, any>,
  durationMs: number
): Promise<void> {
  await logEvent(auditLogId, 'db_update', {
    table,
    rowId,
    fieldsUpdated,
    newValues,
    durationMs
  });
}

export async function logLlmCall(
  auditLogId: number,
  model: string,
  purpose: string,
  inputTokens: number,
  outputTokens: number,
  promptPreview: string,
  responsePreview: string,
  durationMs: number,
  chunkIndex?: number
): Promise<void> {
  await logEvent(auditLogId, 'llm_call', {
    model,
    purpose,
    chunkIndex,
    inputTokens,
    outputTokens,
    promptPreview: promptPreview.substring(0, 200),
    responsePreview: responsePreview.substring(0, 200),
    durationMs
  });
}

export async function logChunkProcessed(
  auditLogId: number,
  chunkIndex: number,
  inputWords: number,
  outputWords: number,
  targetWords: number,
  withinTolerance: boolean,
  claimsAddressed?: string[],
  violations?: string[]
): Promise<void> {
  await logEvent(auditLogId, 'chunk_processed', {
    chunkIndex,
    inputWords,
    outputWords,
    targetWords,
    withinTolerance,
    claimsAddressed,
    violations
  });
}

export async function logSkeletonExtracted(
  auditLogId: number,
  claimsCount: number,
  termsCount: number,
  structuralRequirements: number,
  totalTargetWords: number
): Promise<void> {
  await logEvent(auditLogId, 'skeleton_extracted', {
    claimsCount,
    termsCount,
    structuralRequirements,
    totalTargetWords
  });
}

export async function logStitchPass(
  auditLogId: number,
  coherenceScore: string,
  claimsCovered: number,
  claimsMissing: number,
  topicViolations: number,
  repairsNeeded: number
): Promise<void> {
  await logEvent(auditLogId, 'stitch_pass', {
    coherenceScore,
    claimsCovered,
    claimsMissing,
    topicViolations,
    repairsNeeded
  });
}

export async function logError(
  auditLogId: number,
  errorType: string,
  message: string,
  context: string,
  willRetry: boolean,
  chunkIndex?: number
): Promise<void> {
  await logEvent(auditLogId, 'error', {
    errorType,
    message,
    context,
    chunkIndex,
    willRetry
  });
}

export async function completeAudit(
  auditLogId: number,
  success: boolean,
  finalOutputPreview?: string,
  actualWords?: number,
  targetWords?: number
): Promise<void> {
  await logEvent(auditLogId, 'job_completed', {
    jobType: activeAudits.get(auditLogId)?.jobType || 'unknown',
    targetWords,
    actualWords,
    success
  });
  
  try {
    await db.update(auditLogs)
      .set({
        completedAt: new Date(),
        status: success ? 'completed' : 'failed',
        finalOutputPreview: finalOutputPreview?.substring(0, 500)
      })
      .where(eq(auditLogs.id, auditLogId));
  } catch (error) {
    console.error(`[AUDIT] Failed to complete audit log:`, error);
  }
  
  const audit = activeAudits.get(auditLogId);
  if (audit) {
    Array.from(audit.subscribers).forEach(ws => {
      ws.send(JSON.stringify({ 
        type: 'completed', 
        auditLogId, 
        success,
        totalEntries: audit.entries.length
      }));
    });
  }
  
  activeAudits.delete(auditLogId);
  console.log(`[AUDIT] Completed audit log ${auditLogId}, success: ${success}`);
}

function broadcastEntry(auditLogId: number, entry: AuditLogEntry): void {
  const audit = activeAudits.get(auditLogId);
  if (!audit) return;
  
  const message = JSON.stringify({ type: 'entry', auditLogId, entry });
  
  Array.from(audit.subscribers).forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(message);
    }
  });
}

export async function getAuditLog(auditLogId: number) {
  const [log] = await db.select()
    .from(auditLogs)
    .where(eq(auditLogs.id, auditLogId));
  return log;
}

export async function getAuditEntries(auditLogId: number) {
  return await db.select()
    .from(auditLogEntries)
    .where(eq(auditLogEntries.auditLogId, auditLogId))
    .orderBy(asc(auditLogEntries.sequenceNum));
}

export async function getUserAuditLogs(userId: number, limit: number = 50) {
  return await db.select()
    .from(auditLogs)
    .where(eq(auditLogs.userId, userId))
    .orderBy(desc(auditLogs.startedAt))
    .limit(limit);
}

export async function getFullAuditReport(auditLogId: number) {
  const log = await getAuditLog(auditLogId);
  if (!log) return null;
  
  const entries = await getAuditEntries(auditLogId);
  
  return {
    ...log,
    entries,
    summary: {
      totalEvents: entries.length,
      dbQueries: entries.filter(e => e.eventType === 'db_query').length,
      dbInserts: entries.filter(e => e.eventType === 'db_insert').length,
      dbUpdates: entries.filter(e => e.eventType === 'db_update').length,
      llmCalls: entries.filter(e => e.eventType === 'llm_call').length,
      chunksProcessed: entries.filter(e => e.eventType === 'chunk_processed').length,
      errors: entries.filter(e => e.eventType === 'error').length
    }
  };
}

export function getActiveAuditForJob(jobId: number): number | null {
  const entries = Array.from(activeAudits.entries());
  for (const [auditLogId, audit] of entries) {
    if (audit.jobId === jobId) {
      return auditLogId;
    }
  }
  return null;
}
