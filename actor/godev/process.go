package actor

import (
	"bytes"
	"fmt"
	"github.com/DataDog/gostackparse"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

type Envelope struct {
	Message    any
	Sender *PID
}

// Processer is an interface the abstracts the way a process behaves.
type Processer interface {
	Start()
	PID() *PID
	Send(*PID, any, *PID)
	Invoke([]Envelope)
	Shutdown(*sync.WaitGroup)
}

const (
	procStateRunning int32 = iota
	procStateStopped
)

type process struct {
	Opts

	inbox    Inboxer
	context  *Context
	pid      *PID
	restarts int32

	mbuffer []Envelope
}

func newProcess(e *Engine, opts Opts) *process {
	pid := NewPID(e.address, opts.Kind+pidSeparator+opts.ID)
	ctx := newContext(e, pid)
	p := &process{
		pid:     pid,
		inbox:   NewInbox(opts.InboxSize),
		Opts:    opts,
		context: ctx,
		mbuffer: nil,
	}
	p.inbox.Start(p)
	return p
}

func applyMiddleware(rcv ReceiveFunc, middleware ...MiddlewareFunc) ReceiveFunc {
	for i := len(middleware) - 1; i >= 0; i-- {
		rcv = middleware[i](rcv)
	}
	return rcv
}

func (processRef *process) Invoke(message []Envelope) {
	var (
		// numbers of msgs that need to be processed.
		nmsg = len(message)
		// numbers of msgs that are processed.
		nproc = 0
		// FIXME: We could use nrpoc here, but for some reason placing nproc++ on the
		// bottom of the function it freezes some tests. Hence, I created a new counter
		// for bookkeeping.
		processed = 0
	)
	defer func() {
		// If we recovered, we buffer up all the messages that we could not process
		// so we can retry them on the next restart.
		if v := recover(); v != nil {
			processRef.context.message = Stopped{}
			processRef.context.receiver.Receive(processRef.context)

			processRef.mbuffer = make([]Envelope, nmsg-nproc)
			for i := 0; i < nmsg-nproc; i++ {
				processRef.mbuffer[i] = message[i+nproc]
			}
			processRef.tryRestart(v)
		}
	}()
	for i := 0; i < len(message); i++ {
		nproc++
		msg := message[i]
		if pill, ok := msg.Message.(poisonPill); ok {
			// If we need to gracefuly stop, we process all the messages
			// from the inbox, otherwise we ignore and cleanup.
			if pill.graceful {
				msgsToProcess := message[processed:]
				for _, m := range msgsToProcess {
					processRef.invokeMsg(m)
				}
			}
			processRef.cleanup(pill.wg)
			return
		}
		processRef.invokeMsg(msg)
		processed++
	}
}

func (p *process) invokeMsg(msg Envelope) {
	// suppress poison pill messages here. they're private to the actor engine.
	if _, ok := msg.Message.(poisonPill); ok {
		return
	}
	p.context.message = msg.Message
	p.context.sender = msg.Sender
	recv := p.context.receiver
	if len(p.Opts.Middleware) > 0 {
		applyMiddleware(recv.Receive, p.Opts.Middleware...)(p.context)
	} else {
		recv.Receive(p.context)
	}
}

func (processRef *process) Start() {
	recv := processRef.Producer()
	processRef.context.receiver = recv
	defer func() {
		if value := recover(); value != nil {
			processRef.context.message = Stopped{}
			processRef.context.receiver.Receive(processRef.context)
			processRef.tryRestart(value)
		}
	}()
	processRef.context.message = Initialized{}
	applyMiddleware(recv.Receive, processRef.Opts.Middleware...)(processRef.context)
	processRef.context.engine.BroadcastEvent(ActorInitializedEvent{PID: processRef.pid, Timestamp: time.Now()})

	processRef.context.message = Started{}
	applyMiddleware(recv.Receive, processRef.Opts.Middleware...)(processRef.context)
	processRef.context.engine.BroadcastEvent(ActorStartedEvent{PID: processRef.pid, Timestamp: time.Now()})
	// If we have messages in our buffer, invoke them.
	if len(processRef.mbuffer) > 0 {
		processRef.Invoke(processRef.mbuffer)
		processRef.mbuffer = nil
	}
}

func (processRef *process) tryRestart(v any) {
	// InternalError does not take the maximum restarts into account.
	// For now, InternalError is getting triggered when we are dialing
	// a remote node. By doing this, we can keep dialing until it comes
	// back up. NOTE: not sure if that is the best option. What if that
	// node never comes back up again?
	if message, ok := v.(*InternalError); ok {
		slog.Error(message.From, "err", message.Err)
		time.Sleep(processRef.Opts.RestartDelay)
		processRef.Start()
		return
	}
	stackTrace := cleanTrace(debug.Stack())
	// If we reach the max restarts, we shutdown the inbox and clean
	// everything up.
	if processRef.restarts == processRef.MaxRestarts {
		processRef.context.engine.BroadcastEvent(ActorMaxRestartsExceededEvent{
			PID:       processRef.pid,
			Timestamp: time.Now(),
		})
		processRef.cleanup(nil)
		return
	}

	processRef.restarts++
	// Restart the process after its restartDelay
	processRef.context.engine.BroadcastEvent(ActorRestartedEvent{
		PID:        processRef.pid,
		Timestamp:  time.Now(),
		Stacktrace: stackTrace,
		Reason:     v,
		Restarts:   processRef.restarts,
	})
	time.Sleep(processRef.Opts.RestartDelay)
	processRef.Start()
}

func (processRef *process) cleanup(waitGroup *sync.WaitGroup) {
	if processRef.context.parentCtx != nil {
		processRef.context.parentCtx.children.Delete(processRef.Kind)
	}

	if processRef.context.children.Len() > 0 {
		children := processRef.context.Children()
		for _, pid := range children {
			processRef.context.engine.Poison(pid).Wait()
		}
	}

	processRef.inbox.Stop()
	processRef.context.engine.Registry.Remove(processRef.pid)
	processRef.context.message = Stopped{}
	applyMiddleware(processRef.context.receiver.Receive, processRef.Opts.Middleware...)(processRef.context)

	processRef.context.engine.BroadcastEvent(ActorStoppedEvent{PID: processRef.pid, Timestamp: time.Now()})
	if waitGroup != nil {
		waitGroup.Done()
	}
}

func (p *process) PID() *PID { return p.pid }
func (p *process) Send(_ *PID, msg any, sender *PID) {
	p.inbox.Send(Envelope{Message: msg, Sender: sender})
}
func (p *process) Shutdown(wg *sync.WaitGroup) { p.cleanup(wg) }

func cleanTrace(stack []byte) []byte {
	goros, err := gostackparse.Parse(bytes.NewReader(stack))
	if err != nil {
		slog.Error("failed to parse stacktrace", "err", err)
		return stack
	}
	if len(goros) != 1 {
		slog.Error("expected only one goroutine", "goroutines", len(goros))
		return stack
	}
	// skip the first frames:
	goros[0].Stack = goros[0].Stack[4:]
	buf := bytes.NewBuffer(nil)
	_, _ = fmt.Fprintf(buf, "goroutine %d [%s]\n", goros[0].ID, goros[0].State)
	for _, frame := range goros[0].Stack {
		_, _ = fmt.Fprintf(buf, "%s\n", frame.Func)
		_, _ = fmt.Fprint(buf, "\t", frame.File, ":", frame.Line, "\n")
	}
	return buf.Bytes()
}
