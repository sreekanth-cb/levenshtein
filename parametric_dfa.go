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
	"crypto/md5"
	"encoding/json"
	"log"
	"math"
)

type ParametricState struct {
	shapeID uint32
	offset  uint32
}

func newParametricState() ParametricState {
	return ParametricState{}
}

func (ps *ParametricState) isDeadEnd() bool {
	return ps.shapeID == 0
}

type Transition struct {
	destShapeID uint32
	deltaOffset uint32
}

func (t *Transition) apply(state ParametricState) ParametricState {
	ps := ParametricState{
		shapeID: t.destShapeID}
	// don't need any offset if we are in the dead state,
	// this ensures we have only one dead state.
	if t.destShapeID != 0 {
		ps.offset = state.offset + t.deltaOffset
	}

	return ps
}

type ParametricStateIndex struct {
	stateIndex []uint32
	stateQueue []ParametricState
	numOffsets uint32
}

func newParametricStateIndex(queryLen,
	numParamState uint32) ParametricStateIndex {
	numOffsets := queryLen + 1
	if numParamState == 0 {
		numParamState = numOffsets
	}
	maxNumStates := numParamState * numOffsets
	//log.Printf("numParamState %d numOffsets %d", numParamState, numOffsets)
	psi := ParametricStateIndex{
		stateIndex: make([]uint32, maxNumStates),
		stateQueue: make([]ParametricState, 0, 100),
		numOffsets: numOffsets,
	}

	for i := uint32(0); i < maxNumStates; i++ {
		psi.stateIndex[i] = math.MaxUint32
	}
	return psi
}

func (psi *ParametricStateIndex) numStates() int {
	return len(psi.stateQueue)
}

func (psi *ParametricStateIndex) maxNumStates() int {
	return len(psi.stateIndex)
}

func (psi *ParametricStateIndex) get(stateID uint32) ParametricState {
	return psi.stateQueue[stateID]
}

func (psi *ParametricStateIndex) getOrAllocate(ps ParametricState) uint32 {
	bucket := ps.shapeID*psi.numOffsets + ps.offset
	//log.Printf("bucket %d", bucket)
	if bucket < uint32(len(psi.stateIndex)) && psi.stateIndex[bucket] != math.MaxUint32 {
		return psi.stateIndex[bucket]
	}
	nState := uint32(len(psi.stateQueue))
	psi.stateQueue = append(psi.stateQueue, ps)
	//log.Printf("len %d bucket %d", len(psi.stateIndex), bucket)
	psi.stateIndex[bucket] = nState
	return nState
}

type ParametricDFA struct {
	distance         []uint8
	transitions      []Transition
	maxDistance      uint8
	transitionStride uint32
	diameter         uint32
}

func (pdfa *ParametricDFA) initialState() ParametricState {
	return ParametricState{shapeID: 1}
}

// Returns true iff whatever characters come afterward,
// we will never reach a shorter distance
func (pdfa *ParametricDFA) isPrefixSink(state ParametricState, queryLen uint32) bool {
	if state.isDeadEnd() {
		return true
	}

	remOffset := queryLen - state.offset
	if remOffset < pdfa.diameter {
		stateDistances := pdfa.distance[pdfa.diameter*state.shapeID:]
		prefixDistance := stateDistances[remOffset]
		if prefixDistance > pdfa.maxDistance {
			return false
		}
		//log.Printf("stateDistances %+v", stateDistances[:remOffset])

		for _, d := range stateDistances {
			if d < prefixDistance {
				return false
			}
		}
		return true
	}
	return false
}

func (pdfa *ParametricDFA) numStates() int {
	return len(pdfa.transitions) / int(pdfa.transitionStride)
}

func min(x, y uint32) uint32 {
	if x < y {
		return x
	}
	return y
}

func (pdfa *ParametricDFA) transition(state ParametricState,
	chi uint32) Transition {
	return pdfa.transitions[pdfa.transitionStride*state.shapeID+chi]
}

func (pdfa *ParametricDFA) getDistance(state ParametricState,
	qLen uint32) Distance {
	remainingOffset := qLen - state.offset
	//log.Printf("qLen %d state.offset %d", qLen, state.offset)
	if state.isDeadEnd() || remainingOffset >= pdfa.diameter {
		return Atleast{d: pdfa.maxDistance + 1}
	}
	dist := pdfa.distance[int(pdfa.diameter*state.shapeID)+int(remainingOffset)]
	if dist > pdfa.maxDistance {
		return Atleast{d: dist}
	}
	return Exact{d: dist}
}

func (pdfa *ParametricDFA) computeDistance(left, right string) Distance {
	state := pdfa.initialState()
	leftChars := []rune(left)
	for _, chr := range []rune(right) {
		start := state.offset
		stop := min(start+pdfa.diameter, uint32(len(leftChars)))
		chi := characteristicVector(leftChars[start:stop], chr)
		transistion := pdfa.transition(state, uint32(chi))
		state = transistion.apply(state)
		if state.isDeadEnd() {
			return Atleast{d: pdfa.maxDistance + 1}
		}
	}
	return pdfa.getDistance(state, uint32(len(left)))
}

func (pdfa *ParametricDFA) buildDfa(query string, distance uint8, prefix bool) *DFA {
	qLen := uint32(len([]rune(query)))
	alphabet := queryChars(query)
	//log.Printf("qLen %d alphabet %+v", qLen, alphabet)
	psi := newParametricStateIndex(qLen, uint32(pdfa.numStates()))
	maxNumStates := psi.maxNumStates()
	//log.Printf("maxNumStates %d", maxNumStates)
	deadEndStateID := psi.getOrAllocate(newParametricState())
	if deadEndStateID != 0 {
		return nil
	}
	//log.Printf("deadEndStateID %d", deadEndStateID)
	initialStateID := psi.getOrAllocate(pdfa.initialState())

	dfaBuilder := withMaxStates(uint32(maxNumStates))
	mask := uint32((1 << pdfa.diameter) - 1)
	//log.Printf("initialStateID %d mask %d numStates %d", initialStateID, mask, psi.numStates())

	for stateID := 0; stateID < math.MaxUint32; stateID++ {
		if stateID == psi.numStates() {
			//log.Printf("stateID %d psi.numStates() %d", stateID, psi.numStates())
			//os.Exit(1)
			break
		}
		state := psi.get(uint32(stateID))
		if prefix && pdfa.isPrefixSink(state, qLen) {
			distance := pdfa.getDistance(state, qLen)
			dfaBuilder.addState(uint32(stateID), uint32(stateID), distance)
		} else {
			transition := pdfa.transition(state, 0)
			defSuccessor := transition.apply(state)
			defSuccessorID := psi.getOrAllocate(defSuccessor)
			distance := pdfa.getDistance(state, qLen)
			stateBuilder, err := dfaBuilder.addState(uint32(stateID), defSuccessorID, distance)
			//	log.Printf("\n\ntransition %+v", transition)
			//	log.Printf("defSuccessor %+v state %+v", defSuccessor, state)
			//	log.Printf("defSuccessorID %d", defSuccessorID)
			//	log.Printf("qLen %d distance %d", qLen, distance)
			if err != nil {
				log.Printf("parametric_dfa: buildDfa, err: %v", err)
				return nil
			}

			alphabet.resetNext()
			chr, cv, err := alphabet.next()
			for err == nil {
				chi := cv.shiftAndMask(state.offset, mask)

				transition := pdfa.transition(state, chi)

				destState := transition.apply(state)

				destStateID := psi.getOrAllocate(destState)

				stateBuilder.addTransition(chr, destStateID)
				//	log.Printf("1 chi %d chr %v", chi, chr)
				//	log.Printf("1 destState %+v", destState)
				//	log.Printf("1 destStateID %d", destStateID)
				chr, cv, err = alphabet.next()

				//log.Printf("1 transition %+v", transition)

			}
		}

	}

	dfaBuilder.setInitialState(initialStateID)
	return dfaBuilder.build(distance)
}

func fromNfa(nfa *LevenshteinNFA) *ParametricDFA {
	lookUp := newHash()
	lookUp.getOrAllocate(*newMultiState())
	initialState := nfa.initialStates()
	lookUp.getOrAllocate(*initialState)
	//	log.Printf("initialState %+v newMultiState() %+v", initialState, newMultiState())

	maxDistance := nfa.maxDistance()
	msDiameter := nfa.msDiameter()
	var transitions []Transition
	//log.Printf("maxDistance %d msDiameter %d", maxDistance, msDiameter)

	numChi := 1 << msDiameter
	chiValues := make([]uint64, numChi)
	for i := 0; i < numChi; i++ {
		chiValues[i] = uint64(i)
	}

	//log.Printf("len(lookUp.items) %d chiValues %+v", len(lookUp.items), chiValues)
	for stateID := 0; stateID < math.MaxUint32; stateID++ {
		if stateID == len(lookUp.items) {
			//log.Printf("breaking stateID %d", stateID)
			break
		}
		for _, chi := range chiValues {
			destMs := newMultiState()
			/*	log.Printf("\nitems ")
				for i := range lookUp.items {
					log.Printf(" %+v ", *lookUp.getFromID(i))
				}*/
			ms := lookUp.getFromID(stateID)
			nfa.transition(ms, destMs, chi)

			//	log.Printf("stateID %d before ms %+v destMs %+v", stateID, *ms, destMs)
			translation := destMs.normalize()

			//	log.Printf("after destMs %+v", destMs)

			destID := lookUp.getOrAllocate(*destMs)
			//	log.Printf("destID %d dms %+v chi %d translation %d", destID, destMs, chi, translation)
			transitions = append(transitions, Transition{
				destShapeID: uint32(destID),
				deltaOffset: translation,
			})
		}
	}
	//log.Printf("transitions %d", len(transitions))
	ns := len(lookUp.items)
	//log.Printf("ns %d", ns)
	diameter := int(msDiameter)
	//log.Printf("diameter %d", diameter)
	distances := make([]uint8, 0, diameter*ns)
	for stateID := 0; stateID < ns; stateID++ {
		ms := lookUp.getFromID(stateID)
		for offset := 0; offset < diameter; offset++ {
			dist := nfa.multistateDistance(ms, uint32(offset))
			distances = append(distances, dist.distance())
			//log.Printf("dist.distance() %d", dist.distance())
		}
	}

	return &ParametricDFA{
		diameter:         uint32(msDiameter),
		transitions:      transitions,
		maxDistance:      maxDistance,
		transitionStride: uint32(numChi),
		distance:         distances,
	}
}

type hash struct {
	index map[[16]byte]int
	items []MultiState
}

func newHash() *hash {
	return &hash{
		index: make(map[[16]byte]int, 100),
		items: make([]MultiState, 0, 100),
	}
}

func (h *hash) getOrAllocate(m MultiState) int {
	size := len(h.items)
	var exists bool
	var pos int
	md5 := getHash(&m)
	if pos, exists = h.index[md5]; !exists {
		h.index[md5] = size
		pos = size
		h.items = append(h.items, m)
	}
	return pos
}

func (h *hash) getFromID(id int) *MultiState {
	return &h.items[id]
}

func getHash(ms *MultiState) [16]byte {
	msBytes := []byte{}
	for _, state := range ms.states {
		jsonBytes, _ := json.Marshal(&state)
		msBytes = append(msBytes, jsonBytes...)
	}
	return md5.Sum(msBytes)
}
