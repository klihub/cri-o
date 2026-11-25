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

package adaptation

import (
	"fmt"
	"slices"
	"sync"
)

// registry tracks registered plugins by their indexed name. A name has
// at most one live plugin, the instance which gets relayed requests and
// events. Another instance of the same name replaces it once that
// instance has synchronized. A replaced instance is parked: it is taken
// out of the request and validator paths, but stays connected. A name
// keeps at most maxParked parked plugins, the ones parked most recently.
type registry struct {
	sync.Mutex
	live      map[string]*plugin   // live plugin of each name
	pending   map[string]*plugin   // claimed, not committed yet
	parked    map[string][]*plugin // parked plugins of each name, oldest first
	maxParked int
}

// newRegistry returns an empty plugin registry.
func newRegistry(maxParked int) *registry {
	return &registry{
		live:      make(map[string]*plugin),
		pending:   make(map[string]*plugin),
		parked:    make(map[string][]*plugin),
		maxParked: maxParked,
	}
}

// Claim records a plugin as the pending registration of its name. It
// fails if another instance of that name is already being registered, or
// if the plugin is not allowed to displace the live instance.
func (reg *registry) Claim(p *plugin) error {
	reg.Lock()
	defer reg.Unlock()

	name := p.name()
	if other, ok := reg.pending[name]; ok && other != p {
		return fmt.Errorf("another registration of plugin %q is in progress", name)
	}

	if live, ok := reg.live[name]; ok && live != p {
		if err := mayDisplace(live, p); err != nil {
			return err
		}
	}

	reg.pending[name] = p
	return nil
}

// mayDisplace returns an error if a plugin is not allowed to take over
// the name of the live instance.
func mayDisplace(live, p *plugin) error {
	if p.origin == originExternal && live.origin != originExternal {
		return fmt.Errorf("plugin %q is %s, an external plugin cannot displace it",
			live.name(), live.origin)
	}

	return nil
}

// Commit makes a claimed plugin the live one of its name. It returns the
// instance it replaced, if there was one, and the parked instances of the
// name which are beyond the parked limit.
func (reg *registry) Commit(p *plugin) (*plugin, []*plugin) {
	reg.Lock()
	defer reg.Unlock()

	name := p.name()
	delete(reg.pending, name)

	replaced := reg.live[name]
	reg.live[name] = p

	if replaced == nil || replaced == p {
		return nil, nil
	}

	parked := append(reg.parked[name], replaced)

	// Keep the plugins parked most recently, give up the rest. Parking
	// order decides, so this needs no clock.
	var expired []*plugin
	if excess := len(parked) - reg.maxParked; excess > 0 {
		expired = parked[:excess:excess]
		parked = parked[excess:]
	}
	reg.parked[name] = parked

	return replaced, expired
}

// Release drops a plugin's claim on its name, pending or committed.
func (reg *registry) Release(p *plugin) {
	reg.Lock()
	defer reg.Unlock()

	name := p.name()
	if reg.pending[name] == p {
		delete(reg.pending, name)
	}

	// Check identity before dropping the live entry: a parked instance
	// going away must not take its replacement out of service.
	if reg.live[name] == p {
		delete(reg.live, name)
	}

	if parked, ok := reg.parked[name]; ok {
		parked = slices.DeleteFunc(parked, func(o *plugin) bool { return o == p })
		if len(parked) == 0 {
			delete(reg.parked, name)
		} else {
			reg.parked[name] = parked
		}
	}
}

// Get returns the live plugin of an indexed name, or nil if it has none.
func (reg *registry) Get(idx, base string) *plugin {
	reg.Lock()
	defer reg.Unlock()

	return reg.live[idx+"-"+base]
}
