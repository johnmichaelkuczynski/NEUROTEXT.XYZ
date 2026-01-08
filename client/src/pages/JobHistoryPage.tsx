import { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { 
  Table, 
  TableBody, 
  TableCell, 
  TableHead, 
  TableHeader, 
  TableRow 
} from '@/components/ui/table';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useToast } from '@/hooks/use-toast';
import { 
  History, 
  RefreshCw, 
  Download, 
  Eye, 
  Play, 
  CheckCircle, 
  Clock, 
  AlertCircle,
  FileText,
  Loader2
} from 'lucide-react';

interface Job {
  id: number;
  documentId: string;
  type: 'coherence' | 'reconstruction';
  coherenceMode: string;
  status: 'completed' | 'in-progress' | 'interrupted' | 'failed' | 'processing';
  chunkCount: number;
  createdAt: string;
  lastActivity: string;
  globalState?: any;
  title?: string;
  originalText?: string;
  reconstructedText?: string;
  targetWordCount?: number;
}

interface Chunk {
  id: number;
  chunkIndex: number;
  chunkText: string;
  evaluationResult: any;
  stateAfter: any;
  createdAt: string;
}

interface JobDetail {
  document: any;
  chunks: Chunk[];
  type: string;
}

export function JobHistoryPage() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedJob, setSelectedJob] = useState<Job | null>(null);
  const [jobDetail, setJobDetail] = useState<JobDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [showDetailDialog, setShowDetailDialog] = useState(false);
  const [resuming, setResuming] = useState<string | null>(null);
  const { toast } = useToast();

  const fetchJobs = async () => {
    setLoading(true);
    try {
      const response = await fetch('/api/jobs');
      if (response.ok) {
        const data = await response.json();
        setJobs(data.jobs || []);
      }
    } catch (error) {
      console.error('Error fetching jobs:', error);
      toast({
        title: 'Error',
        description: 'Failed to load job history',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchJobs();
  }, []);

  const fetchJobDetail = async (job: Job) => {
    setDetailLoading(true);
    setSelectedJob(job);
    setShowDetailDialog(true);
    
    try {
      const response = await fetch(`/api/jobs/${encodeURIComponent(job.documentId)}`);
      if (response.ok) {
        const data = await response.json();
        setJobDetail(data);
      }
    } catch (error) {
      console.error('Error fetching job detail:', error);
      toast({
        title: 'Error',
        description: 'Failed to load job details',
        variant: 'destructive',
      });
    } finally {
      setDetailLoading(false);
    }
  };

  const handleResume = async (job: Job) => {
    setResuming(job.documentId);
    try {
      const response = await fetch(`/api/jobs/${encodeURIComponent(job.documentId)}/resume`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });
      
      if (response.ok) {
        const data = await response.json();
        toast({
          title: 'Resume Ready',
          description: `Job can resume from chunk ${data.resumeFromChunk}. Go to Coherence Meter to continue.`,
        });
        
        // Store resume data in sessionStorage for the Coherence Meter to pick up
        sessionStorage.setItem('resumeJob', JSON.stringify({
          documentId: job.documentId,
          coherenceMode: data.coherenceMode,
          resumeFromChunk: data.resumeFromChunk,
          globalState: data.globalState,
          existingChunks: data.existingChunks,
        }));
        
        // Redirect to home page where Coherence Meter is
        window.location.href = '/#coherence-meter';
      }
    } catch (error) {
      console.error('Error resuming job:', error);
      toast({
        title: 'Error',
        description: 'Failed to resume job',
        variant: 'destructive',
      });
    } finally {
      setResuming(null);
    }
  };

  const handleDownload = (job: Job) => {
    let content = '';
    let filename = '';
    
    if (job.type === 'reconstruction') {
      content = job.reconstructedText || job.originalText || '';
      filename = `reconstruction-${job.id}.txt`;
    } else {
      const stitched = job.globalState?.stitchedDocument || '';
      content = stitched;
      filename = `coherence-${job.documentId}.txt`;
    }
    
    if (!content) {
      toast({
        title: 'No output available',
        description: 'This job does not have any output to download.',
        variant: 'destructive',
      });
      return;
    }
    
    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    toast({
      title: 'Downloaded',
      description: `Saved as ${filename}`,
    });
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'completed':
        return <Badge variant="outline" className="bg-green-100 text-green-700 border-green-300 dark:bg-green-900/30 dark:text-green-400 dark:border-green-700"><CheckCircle className="w-3 h-3 mr-1" />Completed</Badge>;
      case 'in-progress':
      case 'processing':
        return <Badge variant="outline" className="bg-amber-100 text-amber-700 border-amber-300 dark:bg-amber-900/30 dark:text-amber-400 dark:border-amber-700"><Clock className="w-3 h-3 mr-1" />In Progress</Badge>;
      case 'interrupted':
        return <Badge variant="outline" className="bg-orange-100 text-orange-700 border-orange-300 dark:bg-orange-900/30 dark:text-orange-400 dark:border-orange-700"><AlertCircle className="w-3 h-3 mr-1" />Interrupted</Badge>;
      case 'failed':
        return <Badge variant="outline" className="bg-red-100 text-red-700 border-red-300 dark:bg-red-900/30 dark:text-red-400 dark:border-red-700"><AlertCircle className="w-3 h-3 mr-1" />Failed</Badge>;
      default:
        return <Badge variant="outline">{status}</Badge>;
    }
  };

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  };

  const formatMode = (mode: string) => {
    return mode
      .replace(/-/g, ' ')
      .replace(/\b\w/g, c => c.toUpperCase());
  };

  return (
    <div className="container mx-auto p-6 max-w-7xl">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <History className="w-8 h-8 text-primary" />
          <h1 className="text-3xl font-bold" data-testid="text-job-history-title">Job History</h1>
        </div>
        <Button 
          onClick={fetchJobs} 
          variant="outline"
          disabled={loading}
          data-testid="button-refresh-jobs"
        >
          <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <FileText className="w-5 h-5" />
            All Jobs
          </CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
            </div>
          ) : jobs.length === 0 ? (
            <div className="text-center py-12 text-muted-foreground">
              <History className="w-12 h-12 mx-auto mb-4 opacity-50" />
              <p>No jobs found. Start processing documents to see them here.</p>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>ID</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Mode</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Chunks</TableHead>
                  <TableHead>Created</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {jobs.map((job) => (
                  <TableRow key={job.documentId} data-testid={`row-job-${job.documentId}`}>
                    <TableCell className="font-mono text-sm">
                      {job.type === 'reconstruction' ? `#${job.id}` : job.documentId.slice(0, 8)}
                    </TableCell>
                    <TableCell>
                      <Badge variant="secondary" className="capitalize">
                        {job.type}
                      </Badge>
                    </TableCell>
                    <TableCell>{formatMode(job.coherenceMode)}</TableCell>
                    <TableCell>{getStatusBadge(job.status)}</TableCell>
                    <TableCell>
                      {job.type === 'coherence' ? `${job.chunkCount} chunks` : '-'}
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {formatDate(job.createdAt)}
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center justify-end gap-2">
                        <Button
                          size="sm"
                          variant="ghost"
                          onClick={() => fetchJobDetail(job)}
                          data-testid={`button-view-${job.documentId}`}
                        >
                          <Eye className="w-4 h-4" />
                        </Button>
                        {job.status === 'interrupted' && job.type === 'coherence' && (
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() => handleResume(job)}
                            disabled={resuming === job.documentId}
                            data-testid={`button-resume-${job.documentId}`}
                          >
                            {resuming === job.documentId ? (
                              <Loader2 className="w-4 h-4 animate-spin" />
                            ) : (
                              <Play className="w-4 h-4" />
                            )}
                          </Button>
                        )}
                        {(job.status === 'completed' || job.globalState?.stitchedDocument || job.reconstructedText) && (
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() => handleDownload(job)}
                            data-testid={`button-download-${job.documentId}`}
                          >
                            <Download className="w-4 h-4" />
                          </Button>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <Dialog open={showDetailDialog} onOpenChange={setShowDetailDialog}>
        <DialogContent className="max-w-4xl max-h-[80vh]">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <FileText className="w-5 h-5" />
              Job Details: {selectedJob?.documentId.slice(0, 12)}...
            </DialogTitle>
          </DialogHeader>
          
          {detailLoading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
            </div>
          ) : jobDetail ? (
            <Tabs defaultValue="overview" className="w-full">
              <TabsList>
                <TabsTrigger value="overview">Overview</TabsTrigger>
                <TabsTrigger value="chunks">Chunks ({jobDetail.chunks.length})</TabsTrigger>
                <TabsTrigger value="output">Final Output</TabsTrigger>
              </TabsList>
              
              <TabsContent value="overview" className="mt-4">
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <p className="text-sm text-muted-foreground">Type</p>
                    <p className="font-medium capitalize">{jobDetail.type}</p>
                  </div>
                  <div className="space-y-2">
                    <p className="text-sm text-muted-foreground">Mode</p>
                    <p className="font-medium">{formatMode(jobDetail.document.coherenceMode || 'reconstruction')}</p>
                  </div>
                  <div className="space-y-2">
                    <p className="text-sm text-muted-foreground">Status</p>
                    {getStatusBadge(selectedJob?.status || 'unknown')}
                  </div>
                  <div className="space-y-2">
                    <p className="text-sm text-muted-foreground">Chunks Processed</p>
                    <p className="font-medium">{jobDetail.chunks.length}</p>
                  </div>
                  <div className="space-y-2">
                    <p className="text-sm text-muted-foreground">Created</p>
                    <p className="font-medium">{formatDate(jobDetail.document.createdAt)}</p>
                  </div>
                </div>
              </TabsContent>
              
              <TabsContent value="chunks" className="mt-4">
                <ScrollArea className="h-[400px]">
                  {jobDetail.chunks.length === 0 ? (
                    <p className="text-center text-muted-foreground py-8">No chunks saved</p>
                  ) : (
                    <div className="space-y-4">
                      {jobDetail.chunks.map((chunk, idx) => (
                        <Card key={chunk.id} className="p-4">
                          <div className="flex items-center justify-between mb-2">
                            <Badge variant="outline">Chunk {chunk.chunkIndex + 1}</Badge>
                            <span className="text-xs text-muted-foreground">
                              {formatDate(chunk.createdAt)}
                            </span>
                          </div>
                          {chunk.chunkText && (
                            <div className="mb-3">
                              <p className="text-xs font-medium text-muted-foreground mb-1">Input Text:</p>
                              <pre className="text-sm bg-muted p-2 rounded max-h-32 overflow-y-auto whitespace-pre-wrap">
                                {chunk.chunkText.slice(0, 500)}{chunk.chunkText.length > 500 ? '...' : ''}
                              </pre>
                            </div>
                          )}
                          {chunk.evaluationResult && (
                            <div>
                              <p className="text-xs font-medium text-muted-foreground mb-1">Evaluation Result:</p>
                              <pre className="text-xs bg-muted p-2 rounded max-h-32 overflow-y-auto">
                                {typeof chunk.evaluationResult === 'string' 
                                  ? chunk.evaluationResult.slice(0, 300)
                                  : JSON.stringify(chunk.evaluationResult, null, 2).slice(0, 300)}
                              </pre>
                            </div>
                          )}
                        </Card>
                      ))}
                    </div>
                  )}
                </ScrollArea>
              </TabsContent>
              
              <TabsContent value="output" className="mt-4">
                <ScrollArea className="h-[400px]">
                  {jobDetail.type === 'reconstruction' ? (
                    jobDetail.document.reconstructedText ? (
                      <pre className="text-sm whitespace-pre-wrap p-4 bg-muted rounded">
                        {jobDetail.document.reconstructedText}
                      </pre>
                    ) : (
                      <p className="text-center text-muted-foreground py-8">No output yet</p>
                    )
                  ) : (
                    jobDetail.document.globalState?.stitchedDocument ? (
                      <pre className="text-sm whitespace-pre-wrap p-4 bg-muted rounded">
                        {jobDetail.document.globalState.stitchedDocument}
                      </pre>
                    ) : (
                      <p className="text-center text-muted-foreground py-8">No stitched output available</p>
                    )
                  )}
                </ScrollArea>
              </TabsContent>
            </Tabs>
          ) : (
            <p className="text-center text-muted-foreground py-8">Failed to load job details</p>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}

export default JobHistoryPage;
