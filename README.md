# levenshtein
levenshtein automaton 

This package makes it fast and simple to build a finite determinic automaton that computes the levenshtein distance from a given string.

# Sample usage:

```

nfa := newLevenshtein(2, false)
pDfa := fromNfa(nfa)

origTerm := "couchbasefts"
dfa := pDfa.buildDfa("couchbases", 2, false)
ed := dfa.eval([]byte(origTerm))
if ed.distance() != 2 {
	log.Errorf("expected distance 2, actual: %d", ed.distance())
}

```

This implementation is inspired by [blog post](https://fulmicoton.com/posts/levenshtein/) and is intended to be
a port of original rust implementation: https://github.com/tantivy-search/levenshtein-automata
