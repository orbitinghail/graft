This crate is designed to be used in testing and development to aid in finding interesting and erroneous states. It can be used in concert with a guidance system to provide high signal fuzz testing.

The crate offers a few features:

1. expectations: a set of macros that can be used to check that the program reaches certain states, and that those states are valid. these macros do not cause the program to crash like an assert would. rather they are emitted to a configured collector for later analysis or a configured guidance system to assist with fuzz testing. notably, these macros support tracking when a state is never reached is extremely helpful for automated fuzzing and verifying test correctness

2. simulation: wrappers around time and randomness to support fast forwarding and better control over randomness when run within a guidance system

3. fault injection: a fault injection system that allows a guidance system to trigger custom faults in a controlled manner
