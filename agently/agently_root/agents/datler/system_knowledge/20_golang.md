# 🧼 Go (Golang) Code Hygiene & Anti-Code-Smell Guide

## 1. Naming Hygiene

### ✅ Do:
- Use concise and meaningful names: `userID`, `configLoader`, `isReady`
- Follow Go naming conventions:
  - CamelCase for **exported** names: `CalculateTotal`
  - lowerCamelCase for **unexported** names: `calculateTotal`
- Use common abbreviations: `id`, `url`, `http`
- Name interfaces with `-er`: `Reader`, `Writer`, `Formatter`

### 🚫 Avoid:
- Redundant names: `HttpClientInstanceStruct`
- Implied prefixes/suffixes: `userStruct`, `loggerInstance`
- Non-descriptive identifiers: `data1`, `tmp`, `foo`

---

## 2. Package Organization

### ✅ Do:
- Keep packages **small and focused**
- Use **noun-based** package names: `auth`, `config`, `logger`
- Avoid stuttering: prefer `auth.Auth()` over `auth.Authenticate()`

### 🚫 Avoid:
- Cyclic dependencies
- Overloaded packages with unrelated logic
- Deep, complex package hierarchies

---

## 3. API and Function Design

### ✅ Do:
- **Return concrete types**, **accept interfaces**
- Write **small, composable functions**
- Wrap errors with context:
  ```go
  return fmt.Errorf("failed to load config: %w", err)
### Avoid:
"God functions" that do too much
Over-exporting: expose only what’s needed
Using interface{} as a shortcut

✅ Do:
Test both success and error paths
Use _test.go and table-driven tests
Keep mocks/interfaces in sync
🚫 Avoid:
  Skipping error cases in tests
  Overuse of mocks (prefer behavior-based tests)

## 5. Common Code Smells

| Smell                 | Symptom                                | Fix                                      |
|-----------------------|-----------------------------------------|-------------------------------------------|
| **God Package**        | `utils` or `common` with unrelated logic | Split by domain-specific packages         |
| **Leaky Abstractions** | Internal details leak into consumers    | Hide logic behind well-defined interfaces |
| **Global Variables**   | Tight coupling, hidden dependencies     | Use dependency injection                  |
| **Fat Interfaces**     | Large, multi-method interfaces          | Favor small interfaces (e.g. `io.Reader`) |
| **Magic Numbers**      | Hardcoded values scattered in code      | Use named constants or typed enums        |

