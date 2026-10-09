using System;
using System.Text;
using System.Runtime.InteropServices;
using System.Threading;
public static class DisposalProbe {
 private static int safe=1;
 public static bool OwnershipSafe {get{return Volatile.Read(ref safe)==1;}}
 [StructLayout(LayoutKind.Sequential,CharSet=CharSet.Unicode)] struct SI {public int cb;public string reserved,desktop,title;public int x,y,xSize,ySize,xCount,yCount,fill,flags;public short show,reserved2;public IntPtr reservedBytes,input,output,error;}
 [StructLayout(LayoutKind.Sequential)] struct PI {public IntPtr process,thread;public uint pid,tid;}
 [StructLayout(LayoutKind.Sequential)] struct BASIC {public long processTime,jobTime;public uint flags;public UIntPtr min,max;public uint active;public UIntPtr affinity;public uint priority,scheduling;}
 [StructLayout(LayoutKind.Sequential)] struct IO {public ulong rOps,wOps,oOps,rBytes,wBytes,oBytes;}
 [StructLayout(LayoutKind.Sequential)] struct EXT {public BASIC basic;public IO io;public UIntPtr processMemory,jobMemory,peakProcess,peakJob;}
 [DllImport("kernel32.dll",SetLastError=true,CharSet=CharSet.Unicode)] static extern IntPtr CreateJobObject(IntPtr sa,string name);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool SetInformationJobObject(IntPtr j,int cls,ref EXT value,int length);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool AssignProcessToJobObject(IntPtr j,IntPtr p);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool IsProcessInJob(IntPtr p,IntPtr j,out bool inside);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool TerminateJobObject(IntPtr j,uint code);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool TerminateProcess(IntPtr p,uint code);
 [DllImport("kernel32.dll",SetLastError=true,CharSet=CharSet.Unicode)] static extern bool CreateProcess(string app,StringBuilder command,IntPtr psa,IntPtr tsa,bool inherit,uint flags,IntPtr env,string cwd,ref SI si,out PI pi);
 [DllImport("kernel32.dll",SetLastError=true)] static extern uint WaitForSingleObject(IntPtr p,uint ms);
 [DllImport("kernel32.dll",SetLastError=true)] static extern bool GetProcessTimes(IntPtr p,out long born,out long exited,out long kernel,out long user);
 [DllImport("kernel32.dll",SetLastError=true)] static extern uint GetProcessId(IntPtr p);
 [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr p);
 public sealed class Receipt {
  public volatile bool created,assigned,inJob,neverResumed=true,initialReturned,workerReady,workerDone,workerJoined,workerFailed,testedReturned,terminationAttempted,terminationSucceeded,processSignalled,identityStable,processClosed,threadClosed,jobClosed,eventsClosed,cacheRestored,complete;
  public uint initialWait,testedWait,cleanupWait;public int initialError,testedError,cleanupError,terminationCount;public long testedElapsedMs;
  public string mode,phase="initial";
 }
 static void Check(bool ok){if(!ok)throw new InvalidOperationException("probe_failure");}
 public static Receipt Run(string exe,bool delayed){
  if(!System.IO.Path.IsPathFullyQualified(exe)||!System.IO.File.Exists(exe)||exe.IndexOf('"')>=0)throw new ArgumentException("probe_input");
  if(Interlocked.CompareExchange(ref safe,0,1)!=1)throw new InvalidOperationException("probe_ownership_unresolved");
  int saved=Marshal.GetLastPInvokeError();var r=new Receipt{mode=delayed?"delayed":"withheld"};IntPtr job=IntPtr.Zero;var pi=new PI();long birth=0;Thread worker=null;ManualResetEvent ready=null,release=null;int terminateClaim=0;
  try {
   r.phase="job";job=CreateJobObject(IntPtr.Zero,null);Check(job!=IntPtr.Zero);var lim=new EXT();lim.basic.flags=0x2000|0x8;lim.basic.active=1;Check(SetInformationJobObject(job,9,ref lim,Marshal.SizeOf<EXT>()));
   r.phase="create-suspended";var si=new SI();si.cb=Marshal.SizeOf<SI>();Check(CreateProcess(exe,new StringBuilder("\""+exe+"\""),IntPtr.Zero,IntPtr.Zero,false,0x4|0x08000000,IntPtr.Zero,null,ref si,out pi));r.created=true;
   r.phase="assign";Check(AssignProcessToJobObject(job,pi.process));r.assigned=true;bool inside;Check(IsProcessInJob(pi.process,job,out inside)&&inside);r.inJob=true;
   long exited,kernel,user;Check(GetProcessTimes(pi.process,out birth,out exited,out kernel,out user)&&GetProcessId(pi.process)==pi.pid);
   r.phase="initial-wait";r.initialWait=WaitForSingleObject(pi.process,0);r.initialError=r.initialWait==uint.MaxValue?Marshal.GetLastWin32Error():0;r.initialReturned=true;Check(r.initialWait==258);
   ready=new ManualResetEvent(false);release=new ManualResetEvent(false);
   worker=new Thread(()=>{try{r.workerReady=true;ready.Set();if(!release.WaitOne(3000)){r.workerFailed=true;return;}if(delayed)Thread.Sleep(100);if(Interlocked.CompareExchange(ref terminateClaim,1,0)!=0){r.workerFailed=true;return;}r.terminationAttempted=true;Interlocked.Increment(ref r.terminationCount);r.terminationSucceeded=TerminateJobObject(job,125);if(!r.terminationSucceeded)r.workerFailed=true;}catch{r.workerFailed=true;}finally{r.workerDone=true;}});worker.IsBackground=true;worker.Start();
   r.phase="worker-ready";Check(ready.WaitOne(1000)&&r.workerReady);
   r.phase="tested-wait";if(delayed)release.Set();var watch=System.Diagnostics.Stopwatch.StartNew();
   r.testedWait=WaitForSingleObject(pi.process,delayed?1000u:50u);r.testedError=r.testedWait==uint.MaxValue?Marshal.GetLastWin32Error():0;r.testedElapsedMs=watch.ElapsedMilliseconds;r.testedReturned=true;
   if(!delayed)Check(r.terminationCount==0&&!r.terminationAttempted);
   Check(delayed?r.testedWait==0&&r.testedElapsedMs>=20:r.testedWait==258);
   r.phase="worker-join";release.Set();r.workerJoined=worker.Join(1000);Check(r.workerJoined&&r.workerDone&&!r.workerFailed&&r.terminationCount==1&&r.terminationSucceeded);
   r.phase="cleanup-wait";r.cleanupWait=WaitForSingleObject(pi.process,1000);r.cleanupError=r.cleanupWait==uint.MaxValue?Marshal.GetLastWin32Error():0;r.processSignalled=r.cleanupWait==0;Check(r.processSignalled);
   long after;r.identityStable=GetProcessTimes(pi.process,out after,out exited,out kernel,out user)&&after==birth&&GetProcessId(pi.process)==pi.pid;Check(r.identityStable);r.phase="complete";r.complete=true;
  }catch{r.complete=false;}
  finally {
   // A live worker retains all shared handles/events. It is never aborted or raced by closure.
   if(worker!=null&&!r.workerJoined){try{release.Set();r.workerJoined=worker.Join(1000);}catch{r.workerFailed=true;}}
   bool canClose=worker==null||r.workerJoined;
   if(canClose){
    if(pi.process!=IntPtr.Zero&&Interlocked.CompareExchange(ref terminateClaim,1,0)==0){r.terminationAttempted=true;Interlocked.Increment(ref r.terminationCount);r.terminationSucceeded=r.assigned?TerminateJobObject(job,125):TerminateProcess(pi.process,125);}
    if(pi.process!=IntPtr.Zero&&!r.processSignalled){r.cleanupWait=WaitForSingleObject(pi.process,1000);r.cleanupError=r.cleanupWait==uint.MaxValue?Marshal.GetLastWin32Error():0;r.processSignalled=r.cleanupWait==0;}
    r.threadClosed=pi.thread==IntPtr.Zero||CloseHandle(pi.thread);r.processClosed=pi.process==IntPtr.Zero||CloseHandle(pi.process);r.jobClosed=job==IntPtr.Zero||CloseHandle(job);
    try{ready?.Dispose();release?.Dispose();r.eventsClosed=true;}catch{r.eventsClosed=false;}
    if((!r.created||r.processSignalled)&&r.threadClosed&&r.processClosed&&r.jobClosed&&r.eventsClosed)Interlocked.Exchange(ref safe,1);
   }
   if(!OwnershipSafe)r.complete=false;Marshal.SetLastPInvokeError(saved);r.cacheRestored=Marshal.GetLastPInvokeError()==saved;
  }
  return r;
 }
}
