//  Copyright (c) 2018 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package levenshtein2

import (
	"fmt"
	"math"
	"reflect"
)

const SinkState = uint32(0)

type DFA struct {
	transitions   [][256]uint32
	distances     []Distance
	initial_state int
	ed            uint8
}

/// Returns the initial state
func (d *DFA) initialState() int {
	return d.initial_state
}

/// Returns the Levenshtein distance associated to the
/// current state.
func (d *DFA) distance(stateId int) Distance {
	//log.Printf("distances %+v", d.distances)
	return d.distances[stateId]
}

/// Returns the number of states in the `DFA`.
func (d *DFA) numStates() int {
	return len(d.transitions)
}

/// Returns the destination state reached after consuming a given byte.
func (d *DFA) transition(fromState int, b uint8) int {
	return int(d.transitions[fromState][b])
}

func (d *DFA) eval(bytes []uint8) Distance {
	state := d.initialState()
	//log.Printf("eval state %+v", state)
	for _, b := range bytes {
		///	log.Printf("b %d", b)
		state = d.transition(state, b)
	}
	//log.Printf("eval final state %+v", state)
	//log.Printf("eval distance len %d", len(d.distances))
	return d.distance(state)
}

func (d *DFA) Start() int {
	return int(d.initialState())
}

func (d *DFA) IsMatch(state int) bool {
	dist := d.distance(state)
	//typ := reflect.TypeOf(dist).String()
	//	log.Printf("typ %s dist %d state %d", typ, dist.distance(), state)
	r := Exact{d: dist.distance()}
	if /*typ == TypeExact &&*/ reflect.DeepEqual(dist, r) /*&& dist.distance() == uint8(d.ed)*/ {
		return true
	}
	return false
}

func (d *DFA) CanMatch(state int) bool {
	//log.Printf("state %d d %d", state, d.numStates())
	return state > 0 && state < d.numStates()
	//return state != int(SinkState) && uint8(d.ed) >= d.distance(state).distance()
}

func (d *DFA) Accept(state int, b byte) int {
	return int(d.transition(state, b))
}

// WillAlwaysMatch returns if the specified state will always end in a
// matching state.
func (d *DFA) WillAlwaysMatch(state int) bool {
	return false
}

func fill(dest []uint32, val uint32) {
	for i := range dest {
		dest[i] = val
	}
}

func fillTransitions(dest *[256]uint32, val uint32) {
	for i := range dest {
		dest[i] = val
	}
}

type Utf8DFAStateBuilder struct {
	dfaBuilder       *Utf8DFABuilder
	stateID          uint32
	defaultSuccessor []uint32
}

func (sb *Utf8DFAStateBuilder) addTransitionID(fromStateID uint32, b uint8,
	toStateID uint32) {
	//log.Printf("SET transitions fromStateID %d b %d =>  %d", fromStateID, b, toStateID)
	sb.dfaBuilder.transitions[fromStateID][b] = toStateID
}

func (sb *Utf8DFAStateBuilder) addTransition(in rune, toStateID uint32) {
	fromStateID := sb.stateID
	chars := []byte(string(in))
	lastByte := chars[len(chars)-1]
	//	log.Printf("bytes %+v fromStateID %d sb.defaultSuccessor %+v", chars, fromStateID, sb.defaultSuccessor)
	for i, ch := range chars[:len(chars)-1] {
		remNumBytes := len(chars) - i - 1
		defaultSuccessor := sb.defaultSuccessor[remNumBytes]
		intermediateStateID := sb.dfaBuilder.transitions[fromStateID][ch]

		//	log.Printf("intermediateStateID %d defaultSuccessor %d  remNumBytes  %d", intermediateStateID, defaultSuccessor, remNumBytes)
		if intermediateStateID == defaultSuccessor {
			intermediateStateID = sb.dfaBuilder.allocate()
			//	log.Printf("inside intermediateStateID %d ", intermediateStateID)
			fillTransitions(&sb.dfaBuilder.transitions[intermediateStateID], sb.defaultSuccessor[remNumBytes-1])
		}

		//	log.Printf("outside intermediateStateID %d ", intermediateStateID)
		sb.addTransitionID(fromStateID, ch, intermediateStateID)
		fromStateID = intermediateStateID
	}

	toStateIDDecoded := sb.dfaBuilder.getOrAllocate(original(toStateID))
	//	log.Printf("toStateIDDecoded %d fromStateID %d lastByte %d", toStateIDDecoded, fromStateID, lastByte)
	sb.addTransitionID(fromStateID, lastByte, toStateIDDecoded)
}

type Utf8StateId uint32

func original(stateId uint32) Utf8StateId {
	return predecessor(stateId, 0)
}

func predecessor(stateId uint32, numSteps uint8) Utf8StateId {
	return Utf8StateId(stateId*4 + uint32(numSteps))
}

// Utf8DFABuilder makes it possible to define a DFA
// that takes unicode character, and build a `DFA`
// that operates on utf-8 encoded
type Utf8DFABuilder struct {
	index        []uint32
	distances    []Distance
	transitions  [][256]uint32
	initialState uint32
	numStates    uint32
	maxNumStates uint32
}

func withMaxStates(maxStates uint32) *Utf8DFABuilder {
	rv := &Utf8DFABuilder{
		index:        make([]uint32, maxStates*4+3),
		distances:    make([]Distance, 0, 100),
		transitions:  make([][256]uint32, 0, 100),
		maxNumStates: maxStates,
	}

	for i := range rv.index {
		rv.index[i] = math.MaxUint32
	}

	return rv
}

func (dfab *Utf8DFABuilder) allocate() uint32 {
	newState := dfab.numStates
	dfab.numStates++
	//log.Printf("extending distances from %d", len(dfab.distances))
	dfab.distances = append(dfab.distances, Atleast{d: 255})
	//log.Printf("to %d", len(dfab.distances))
	// TODO
	var transition [256]uint32
	/*if int(newState) >= len(dfab.transitions) {
		transitions := make([][256]uint32, newState+1)
		dfab.transitions = append(dfab.transitions, transitions...)
	}  else {*/
	dfab.transitions = append(dfab.transitions, transition)
	//}

	return newState
}

func (dfab *Utf8DFABuilder) getOrAllocate(state Utf8StateId) uint32 {
	if dfab.index[state] != math.MaxUint32 {
		return dfab.index[state]
	}
	nstate := dfab.allocate()
	dfab.index[state] = nstate
	//	log.Printf("allocate dfab.distances %+v nstate %d", dfab.distances, nstate)
	//log.Printf("allocate dfab.transitions %d nstate %d", len(dfab.transitions), nstate)
	return nstate
}

func (dfab *Utf8DFABuilder) setInitialState(iState uint32) {
	decodedID := dfab.getOrAllocate(original(iState))
	//log.Printf("decodedID %d", decodedID)
	dfab.initialState = decodedID
}

func (dfab *Utf8DFABuilder) build(ed uint8) *DFA {
	return &DFA{
		transitions:   dfab.transitions,
		distances:     dfab.distances,
		initial_state: int(dfab.initialState),
		ed:            ed,
	}
}

func (dfab *Utf8DFABuilder) addState(state, default_suc_orig uint32,
	distance Distance) (*Utf8DFAStateBuilder, error) {
	if state > dfab.maxNumStates {
		return nil, fmt.Errorf("State id is larger than maxNumStates")
	}

	stateID := dfab.getOrAllocate(original(state))
	dfab.distances[stateID] = distance

	defaultSuccID := dfab.getOrAllocate(original(default_suc_orig))
	// creates a chain of states of predecessors of `default_suc_orig`.
	// Accepting k-bytes (whatever the bytes are) from `predecessor_states[k-1]`
	// leads to the `default_suc_orig` state.

	predecessorStates := []uint32{defaultSuccID, defaultSuccID, defaultSuccID, defaultSuccID}

	for numBytes := uint8(1); numBytes < 4; numBytes++ {

		predecessorState := predecessor(default_suc_orig, numBytes)
		predecessorStateID := dfab.getOrAllocate(predecessorState)
		predecessorStates[numBytes] = predecessorStateID
		succ := predecessorStates[numBytes-1]
		//log.Printf("transitions predecessor_state_id fiiled %d", predecessorStateID)
		fillTransitions(&dfab.transitions[predecessorStateID], succ)
	}

	//log.Printf("transitions stateID fiiled %d", stateID)
	//transitions := dfab.transitions[stateID]
	// 1-byte encoded chars.
	fill(dfab.transitions[stateID][0:192], predecessorStates[0])
	// 2-bytes encoded chars.
	fill(dfab.transitions[stateID][192:224], predecessorStates[1])
	// 3-bytes encoded chars.
	fill(dfab.transitions[stateID][224:240], predecessorStates[2])
	// 4-bytes encoded chars.
	fill(dfab.transitions[stateID][240:256], predecessorStates[3])

	//	log.Printf("predecessorStates %+v", predecessorStates)
	return &Utf8DFAStateBuilder{
		dfaBuilder:       dfab,
		stateID:          stateID,
		defaultSuccessor: predecessorStates}, nil
}
