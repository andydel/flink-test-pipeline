# Tasks: [FEATURE NAME]

**Input**: Design documents from `/specs/[###-feature-name]/`
**Prerequisites**: plan.md (required), research.md, data-model.md, contracts/

## Execution Flow (main)
```
1. Load plan.md from feature directory
   → If not found: ERROR "No implementation plan found"
   → Extract: tech stack, libraries, structure
2. Load optional design documents:
   → data-model.md: Extract entities → model tasks
   → contracts/: Each file → contract test task
   → research.md: Extract decisions → setup tasks
3. Generate tasks by category:
   → Setup: project init, Flink dependencies, schema registry
   → Tests: stream tests, fault tolerance tests, end-to-end pipeline tests
   → Core: stream transformations, operators, windowing logic
   → Integration: sources, sinks, state management, checkpointing
   → Monitoring: metrics, logging, health checks, performance validation
4. Apply task rules:
   → Different files = mark [P] for parallel
   → Same file = sequential (no [P])
   → Tests before implementation (TDD)
5. Number tasks sequentially (T001, T002...)
6. Generate dependency graph
7. Create parallel execution examples
8. Validate task completeness:
   → All contracts have tests?
   → All entities have models?
   → All endpoints implemented?
9. Return: SUCCESS (tasks ready for execution)
```

## Format: `[ID] [P?] Description`
- **[P]**: Can run in parallel (different files, no dependencies)
- Include exact file paths in descriptions

## Path Conventions
- **Single project**: `src/`, `tests/` at repository root
- **Web app**: `backend/src/`, `frontend/src/`
- **Mobile**: `api/src/`, `ios/src/` or `android/src/`
- Paths shown below assume single project - adjust based on plan.md structure

## Phase 3.1: Setup
- [ ] T001 Create project structure per implementation plan
- [ ] T002 Initialize [language] project with [framework] dependencies
- [ ] T003 [P] Configure linting and formatting tools

## Phase 3.2: Tests First (TDD) ⚠️ MUST COMPLETE BEFORE 3.3
**CRITICAL: These tests MUST be written and MUST FAIL before ANY implementation**
- [ ] T004 [P] Stream processing test for data transformation in tests/stream/test_transform.py
- [ ] T005 [P] Fault tolerance test with checkpoint recovery in tests/fault/test_recovery.py
- [ ] T006 [P] End-to-end pipeline test with backpressure in tests/e2e/test_pipeline.py
- [ ] T007 [P] Schema evolution test with version compatibility in tests/schema/test_evolution.py

## Phase 3.3: Core Implementation (ONLY after tests are failing)
- [ ] T008 [P] Data model schemas in src/schemas/events.avro
- [ ] T009 [P] Stream transformation operators in src/operators/transform.py
- [ ] T010 [P] Windowing functions in src/windows/tumbling.py
- [ ] T011 Source connector for data ingestion in src/sources/kafka_source.py
- [ ] T012 Sink connector for data output in src/sinks/elasticsearch_sink.py
- [ ] T013 Input validation
- [ ] T014 Error handling and logging

## Phase 3.4: Integration
- [ ] T015 Configure Flink state backend with checkpointing
- [ ] T016 Implement exactly-once semantics
- [ ] T017 Setup structured logging with correlation IDs
- [ ] T018 Configure monitoring and alerting

## Phase 3.5: Polish
- [ ] T019 [P] Property-based tests for data transformations in tests/property/test_transforms.py
- [ ] T020 Performance tests (throughput and latency validation)
- [ ] T021 [P] Update pipeline documentation in docs/pipeline.md
- [ ] T022 Remove duplication
- [ ] T023 Run manual-testing.md

## Dependencies
- Tests (T004-T007) before implementation (T008-T014)
- T008 blocks T009, T015
- T016 blocks T018
- Implementation before polish (T019-T023)

## Parallel Example
```
# Launch T004-T007 together:
Task: "Stream processing test for data transformation in tests/stream/test_transform.py"
Task: "Fault tolerance test with checkpoint recovery in tests/fault/test_recovery.py"
Task: "End-to-end pipeline test with backpressure in tests/e2e/test_pipeline.py"
Task: "Schema evolution test with version compatibility in tests/schema/test_evolution.py"
```

## Notes
- [P] tasks = different files, no dependencies
- Verify tests fail before implementing
- Commit after each task
- Avoid: vague tasks, same file conflicts

## Task Generation Rules
*Applied during main() execution*

1. **From Contracts**:
   - Each contract file → contract test task [P]
   - Each endpoint → implementation task
   
2. **From Data Model**:
   - Each entity → model creation task [P]
   - Relationships → service layer tasks
   
3. **From User Stories**:
   - Each story → integration test [P]
   - Quickstart scenarios → validation tasks

4. **Ordering**:
   - Setup → Tests → Models → Services → Endpoints → Polish
   - Dependencies block parallel execution

## Validation Checklist
*GATE: Checked by main() before returning*

- [ ] All stream transformations have corresponding tests
- [ ] All schemas have validation and evolution tasks
- [ ] All tests come before implementation (TDD)
- [ ] Fault tolerance and monitoring tasks included
- [ ] Parallel tasks truly independent
- [ ] Each task specifies exact file path
- [ ] No task modifies same file as another [P] task