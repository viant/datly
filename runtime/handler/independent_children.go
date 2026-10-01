package handler

// IndependentChildOrchestrator identifies custom adapters eligible for explicit
// source-less orchestration. Engine ownership guards remain mandatory.
type IndependentChildOrchestrator interface{ SupportsIndependentChildTransactions() bool }
