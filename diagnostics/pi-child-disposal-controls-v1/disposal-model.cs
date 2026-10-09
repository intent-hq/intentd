using System;
using System.Collections.Generic;
public static class DisposalModel {
 public sealed class State {
  public bool childWaitAttempted,childWaitReturned,childWaitHandlePresent,childSignalledAfter,childIdentityStable,childHandleClosed;
  public uint childWaitResult,childPid=23;public string childCreation="222";public bool FaultPre,FaultPost;
  private string stage="not-run";private int error;
  public string childWaitStage {get{return stage;}set{if(FaultPre)throw new InvalidOperationException("pre-store");stage=value;}}
  public int childWaitError {get{return error;}set{if(FaultPost)throw new InvalidOperationException("post-store");error=value;}}
 }
 public sealed class Clock {public long ElapsedMilliseconds;public void Advance(long value){ElapsedMilliseconds=checked(ElapsedMilliseconds+value);}}
 public sealed class Result {public State state;public Exception error;public string[] ledger;public uint requested,returned;public bool cacheRestored,contextRestored;public int waits;public long charged;}
 [ThreadStatic] static Harness current;
 static class Marshal {public static int GetLastWin32Error(){current.calls.Add("cached-error");return System.Runtime.InteropServices.Marshal.GetLastWin32Error();}}
 sealed class Harness {
  internal List<string> calls=new List<string>();internal Clock disposal=new Clock();internal Result result;internal string mode,fault;internal long delay;internal Exception injected;internal bool sameHandle=true;
  uint WaitForSingleObject(IntPtr h,uint ms){sameHandle&=h==new IntPtr(42);result.waits++;result.requested=ms;calls.Add("wait:"+ms);if(fault=="wait")throw injected;System.Runtime.InteropServices.Marshal.SetLastPInvokeError(6);uint code=mode=="failed"?uint.MaxValue:mode=="other"?128u:mode=="never"?258u:delay<=ms?0u:258u;result.returned=code;return code;}
  bool GetProcessTimes(IntPtr h,out long born,out long exited,out long kernel,out long user){sameHandle&=h==new IntPtr(42);calls.Add("identity");if(fault=="identity")throw injected;born=fault=="wrong-birth"?223:222;exited=kernel=user=0;System.Runtime.InteropServices.Marshal.SetLastPInvokeError(999);return true;}
  uint GetProcessId(IntPtr h){sameHandle&=h==new IntPtr(42);calls.Add("pid");return fault=="wrong-pid"?24u:23u;}
  bool CloseHandle(IntPtr h){sameHandle&=h==new IntPtr(42);calls.Add("close");if(fault=="close")throw injected;return fault!="close-false";}
  internal void Original(State r){IntPtr childHandle=new IntPtr(42);
   if(childHandle!=IntPtr.Zero){
    long born,exited,kernel,user;
    try {r.childWaitStage="after-reader-before-child-close";r.childWaitHandlePresent=true;r.childWaitAttempted=true;}catch{}
    uint childWaitResult=WaitForSingleObject(childHandle,0);
    r.childSignalledAfter=childWaitResult==0;
    // No intervening native call; SetLastError=true preserves this wait's error for WAIT_FAILED only.
    try {r.childWaitError=childWaitResult==uint.MaxValue?Marshal.GetLastWin32Error():0;r.childWaitResult=childWaitResult;r.childWaitReturned=true;}catch{}
    r.childIdentityStable=GetProcessTimes(childHandle,out born,out exited,out kernel,out user)&&born.ToString(System.Globalization.CultureInfo.InvariantCulture)==r.childCreation&&GetProcessId(childHandle)==r.childPid;
    r.childHandleClosed=CloseHandle(childHandle);childHandle=IntPtr.Zero;
   }
  }
  internal void Candidate(State r){IntPtr childHandle=new IntPtr(42);var disposal=this.disposal;
   if(childHandle!=IntPtr.Zero){
    long born,exited,kernel,user;
    try {r.childWaitStage="after-reader-before-child-close";r.childWaitHandlePresent=true;r.childWaitAttempted=true;}catch{}
    uint childWaitResult=WaitForSingleObject(childHandle,(uint)Math.Max(0L,10000L-disposal.ElapsedMilliseconds));
    r.childSignalledAfter=childWaitResult==0;
    // No intervening native call; SetLastError=true preserves this wait's error for WAIT_FAILED only.
    try {r.childWaitError=childWaitResult==uint.MaxValue?Marshal.GetLastWin32Error():0;r.childWaitResult=childWaitResult;r.childWaitReturned=true;}catch{}
    r.childIdentityStable=GetProcessTimes(childHandle,out born,out exited,out kernel,out user)&&born.ToString(System.Globalization.CultureInfo.InvariantCulture)==r.childCreation&&GetProcessId(childHandle)==r.childPid;
    r.childHandleClosed=CloseHandle(childHandle);childHandle=IntPtr.Zero;
   }
  }
 }
 public static uint MutantBudget(long elapsed,string kind){if(kind=="constant")return 10000;if(kind=="restart-after-members")return (uint)Math.Max(0L,10000L-(elapsed-6000L));if(kind=="unsigned-underflow")return unchecked((uint)(10000L-elapsed));throw new ArgumentException("mutant");}
 public static Result Run(bool candidate,long[] charges,long delay,string mode,string fault,Exception injected){
  var result=new Result{state=new State{FaultPre=fault=="pre",FaultPost=fault=="post"}};var h=new Harness{result=result,delay=delay,mode=mode,fault=fault,injected=injected};
  var previous=current;int saved=System.Runtime.InteropServices.Marshal.GetLastPInvokeError();current=h;
  try {h.calls.Add("clock-start");if(charges.Length!=4)throw new ArgumentException("charges");var labels=new[]{"termination-members","root","reader","dispose"};for(int i=0;i<4;i++){if(charges[i]<0)throw new ArgumentException("charge-negative");h.disposal.Advance(charges[i]);h.calls.Add("charge:"+labels[i]+":"+charges[i]);}result.charged=h.disposal.ElapsedMilliseconds;if(candidate)h.Candidate(result.state);else h.Original(result.state);if(!h.sameHandle)throw new InvalidOperationException("handle-substitution");}
  catch(Exception error){result.error=error;}
  finally{current=previous;result.contextRestored=object.ReferenceEquals(current,previous);System.Runtime.InteropServices.Marshal.SetLastPInvokeError(saved);result.cacheRestored=System.Runtime.InteropServices.Marshal.GetLastPInvokeError()==saved;}
  result.ledger=h.calls.ToArray();return result;
 }
}
