/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package nri

// PROTOTYPE. The order this integration enters NRI in and applies what NRI
// answers with, kept by handing a baton along rather than by holding the
// one local lock across a request.
//
// The lock this replaces wrapped the whole NRI call, so it admitted one
// request at a time and NRI's own ordering work was wasted: removing NRI's
// lock alone buys nothing while this one is still here.
//
// Only requests a plugin can answer with a change need an order:
// CreateContainer, UpdateContainer and StopContainer. For the rest a plugin
// says proceed or fail, so there is nothing to apply and nothing to order.
//
// Two ordering points per request:
//
//   - enter, so requests reach NRI in this order and NRI's own order
//     agrees with it. The baton passes as soon as the request has been
//     issued, not when NRI answers, so the next request follows it in
//     rather than waiting out its plugins.
//   - commit, so the cross-container decisions NRI returns -- the updates
//     and the evictions -- are applied in the order the requests were
//     issued in. This is the guarantee: a plugin that moves container A
//     and B in answer to one request and moves them again in answer to the
//     next has both answers applied in that order.
//
// An adjustment needs no such care. It describes the container the request
// is about, which no other request touches, and the caller applies it to
// that container's own spec. Only the updates and evictions reach across
// containers, and those are applied here, inside the commit.
//
// A baton is a channel its holder closes, so a holder that gives up on an
// error or a cancelled context still passes it on and the chain never
// stalls.

import (
	"context"
	"sync"
)

// baton is one request's turn in one chain.
//
// The zero baton is a turn nobody is waiting for: waiting on it returns at
// once and passing it does nothing.
type baton struct {
	prev <-chan struct{}
	mine chan struct{}
}

// wait blocks until the previous holder passed the baton, or ctx ends.
func (b baton) wait(ctx context.Context) error {
	if b.prev == nil {
		return nil
	}

	select {
	case <-b.prev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// pass hands the baton to the next holder. Safe to call more than once.
func (b baton) pass() {
	if b.mine == nil {
		return
	}

	select {
	case <-b.mine:
	default:
		close(b.mine)
	}
}

// chain holds the baton of the request that most recently joined it.
type chain struct {
	last chan struct{}
}

// join adds a holder to the end of the chain.
func (c *chain) join() baton {
	b := baton{prev: c.last, mine: make(chan struct{})}
	c.last = b.mine

	return b
}

// requestOrder is the order requests enter NRI in and the order their
// answers are applied in.
type requestOrder struct {
	mu     sync.Mutex
	enter  chain
	commit chain
}

// turn is one request's place in that order.
type turn struct {
	enter  baton
	commit baton
}

// join puts a request at the end of both chains in one step, so that the
// order requests enter NRI in is the order their answers are applied in.
func (o *requestOrder) join() *turn {
	o.mu.Lock()
	defer o.mu.Unlock()

	return &turn{
		enter:  o.enter.join(),
		commit: o.commit.join(),
	}
}

// joinCommit puts a holder at the end of the commit chain only, for a
// decision that arrives already made and only needs applying in turn.
func (o *requestOrder) joinCommit() *turn {
	o.mu.Lock()
	defer o.mu.Unlock()

	return &turn{commit: o.commit.join()}
}

// release passes every baton this request still holds, so a request that
// returns early for any reason leaves no chain waiting on it.
func (t *turn) release() {
	t.enter.pass()
	t.commit.pass()
}

// enterTurn waits for this request's turn to call NRI and passes the baton
// on, so the call itself is not in the next request's way.
func (t *turn) enterTurn(ctx context.Context) error {
	if err := t.enter.wait(ctx); err != nil {
		return err
	}
	t.enter.pass()

	return nil
}

// commitTurn waits until every earlier request has applied its answer.
func (t *turn) commitTurn(ctx context.Context) error {
	return t.commit.wait(ctx)
}
