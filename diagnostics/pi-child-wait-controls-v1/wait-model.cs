using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;
public static class WaitDiagnosticModel {
 public sealed class State {
  public bool childWaitAttempted,childWaitReturned,childWaitHandlePresent,childSignalledAfter,childIdentityStable,childHandleClosed;
  public uint childWaitResult;public uint childPid=23;public string childCreation="222";
  public bool FaultPre,FaultPost;
  private string stage="not-run";private int error;
  public string childWaitStage {get{return stage;}set{if(FaultPre)throw new InvalidOperationException("pre-store");stage=value;}}
  public int childWaitError {get{return error;}set{if(FaultPost)throw new InvalidOperationException("post-store");error=value;}}
 }
 public sealed class Result {public State state;public Exception error;public string[] ledger;public Exception injected;public bool cachedErrorRestored,contextRestored;}
 [ThreadStatic] static Harness current;
 // This model shim records the read but delegates to the actual CLR cached-error API.
 static class Marshal {public static int GetLastWin32Error(){current.calls.Add("cached-error");return System.Runtime.InteropServices.Marshal.GetLastWin32Error();}}
 sealed class Harness {
  internal List<string> calls=new List<string>();internal uint result;internal int error;internal string failAt;internal Exception injected;
  uint WaitForSingleObject(IntPtr h,uint ms){calls.Add("wait:"+ms);if(failAt=="wait")throw injected;System.Runtime.InteropServices.Marshal.SetLastPInvokeError(error);return result;}
  bool GetProcessTimes(IntPtr h,out long born,out long exited,out long kernel,out long user){calls.Add("identity");if(failAt=="identity")throw injected;born=222;exited=kernel=user=0;System.Runtime.InteropServices.Marshal.SetLastPInvokeError(999);return true;}
  uint GetProcessId(IntPtr h){calls.Add("pid");return 23;}
  bool CloseHandle(IntPtr h){calls.Add("close");if(failAt=="close")throw injected;return true;}
  internal void Original(State r){IntPtr childHandle=new IntPtr(42);
   if(childHandle!=IntPtr.Zero){
    long born,exited,kernel,user;r.childSignalledAfter=WaitForSingleObject(childHandle,0)==0;
    r.childIdentityStable=GetProcessTimes(childHandle,out born,out exited,out kernel,out user)&&born.ToString(System.Globalization.CultureInfo.InvariantCulture)==r.childCreation&&GetProcessId(childHandle)==r.childPid;
    r.childHandleClosed=CloseHandle(childHandle);childHandle=IntPtr.Zero;
   }
  }
  internal void Candidate(State r){IntPtr childHandle=new IntPtr(42);
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
 }
 public static Result Run(bool candidate,uint waitResult,int cachedError,string fault,string failAt,Exception injected){
  var h=new Harness{result=waitResult,error=cachedError,failAt=failAt,injected=injected};var state=new State{FaultPre=fault=="pre",FaultPost=fault=="post"};var result=new Result{state=state,injected=injected};
  Harness previous=current;int savedError=System.Runtime.InteropServices.Marshal.GetLastPInvokeError();current=h;
  try{if(candidate)h.Candidate(state);else h.Original(state);}catch(Exception error){result.error=error;}
  finally{current=previous;result.contextRestored=object.ReferenceEquals(current,previous);System.Runtime.InteropServices.Marshal.SetLastPInvokeError(savedError);result.cachedErrorRestored=System.Runtime.InteropServices.Marshal.GetLastPInvokeError()==savedError;}
  result.ledger=h.calls.ToArray();return result;
 }
}
