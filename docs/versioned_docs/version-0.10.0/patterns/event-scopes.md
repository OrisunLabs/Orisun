---
title: Event Scopes
description: Model event membership with queryable scope keys.
---

Event scopes are a modeling convention for grouping related events without forcing every command into one stream. The event that starts something gets its own stable event id. Later events copy that id into queryable scope fields.

This pattern is adapted from Ralf Westphal's article [Scoping Events](https://ralfwestphal.substack.com/p/scoping-events), especially the course-enrollment example. Westphal describes scopes as event-rooted containers: every event can be the root of a scope, and every event can be a member of multiple scopes.

Orisun does not reserve a special `scope` or `scopes` field. Criteria and indexes match JSON keys in event `data`, so scope keys are normal event data keys. The examples below use `scopes.` as a naming convention. When calling `SaveEvents`, pass the type through `event_type`; Orisun writes the canonical `eventType` key into `data`.

## Course enrollment chain

Start with two independent scope roots:

```json
{
  "eventType": "StudentRegistered",
  "studentRegisteredId": "018f2d5e-1001-7000-8000-000000000001",
  "studentName": "Mary"
}
```

```json
{
  "eventType": "CoursePublished",
  "coursePublishedId": "018f2d5e-1002-7000-8000-000000000002",
  "courseNumber": "25.2.63.101",
  "title": "Event Sourcing 101"
}
```

The enrollment event belongs to both roots. It also becomes a new scope root of its own:

```json
{
  "eventType": "StudentEnrolledInCourse",
  "studentEnrolledInCourseId": "018f2d5e-1003-7000-8000-000000000003",
  "enrolledAt": "2026-06-22T09:00:00Z",
  "scopes.studentRegisteredId": "018f2d5e-1001-7000-8000-000000000001",
  "scopes.coursePublishedId": "018f2d5e-1002-7000-8000-000000000002"
}
```

A grade belongs directly to the enrollment scope and also carries the outer student and course scopes. Carrying the outer scopes is redundant from a graph-theory perspective, but it keeps event-store queries simple and indexable:

```json
{
  "eventType": "GradeAssigned",
  "gradeAssignedId": "018f2d5e-1004-7000-8000-000000000004",
  "grade": "B+",
  "assignedAt": "2026-06-22T10:00:00Z",
  "scopes.studentEnrolledInCourseId": "018f2d5e-1003-7000-8000-000000000003",
  "scopes.studentRegisteredId": "018f2d5e-1001-7000-8000-000000000001",
  "scopes.coursePublishedId": "018f2d5e-1002-7000-8000-000000000002"
}
```

The article then extends the example: a grade dispute starts a chat in the scope of `GradeAssigned`. The chat is a new nested scope root, but it still carries the wider course context:

```json
{
  "eventType": "ChatStarted",
  "chatStartedId": "018f2d5e-1005-7000-8000-000000000005",
  "topic": "Grade discussion",
  "startedAt": "2026-06-22T11:00:00Z",
  "scopes.gradeAssignedId": "018f2d5e-1004-7000-8000-000000000004",
  "scopes.studentEnrolledInCourseId": "018f2d5e-1003-7000-8000-000000000003",
  "scopes.studentRegisteredId": "018f2d5e-1001-7000-8000-000000000001",
  "scopes.coursePublishedId": "018f2d5e-1002-7000-8000-000000000002"
}
```

Messages happen inside the chat scope. They can also carry inherited scopes so a query from the course, student, enrollment, grade, or chat perspective can all be a simple content query:

```json
{
  "eventType": "ChatMessageSent",
  "chatMessageSentId": "018f2d5e-1006-7000-8000-000000000006",
  "sender": "mary",
  "message": "Can we review the grading rubric?",
  "sentAt": "2026-06-22T11:05:00Z",
  "scopes.chatStartedId": "018f2d5e-1005-7000-8000-000000000005",
  "scopes.gradeAssignedId": "018f2d5e-1004-7000-8000-000000000004",
  "scopes.studentEnrolledInCourseId": "018f2d5e-1003-7000-8000-000000000003",
  "scopes.studentRegisteredId": "018f2d5e-1001-7000-8000-000000000001",
  "scopes.coursePublishedId": "018f2d5e-1002-7000-8000-000000000002"
}
```

The same course scope can also contain events that are not part of one student's enrollment chain. For example, course feedback or agenda questions can sit directly in the `CoursePublished` scope:

```json
{
  "eventType": "CourseLiked",
  "courseLikedId": "018f2d5e-1007-7000-8000-000000000007",
  "likedBy": "018f2d5e-1001-7000-8000-000000000001",
  "likedAt": "2026-06-22T12:00:00Z",
  "scopes.coursePublishedId": "018f2d5e-1002-7000-8000-000000000002"
}
```

```json
{
  "eventType": "CourseQuestionAsked",
  "courseQuestionAskedId": "018f2d5e-1008-7000-8000-000000000008",
  "askedBy": "018f2d5e-1001-7000-8000-000000000001",
  "question": "Will there be a session on projections?",
  "askedAt": "2026-06-22T12:10:00Z",
  "scopes.coursePublishedId": "018f2d5e-1002-7000-8000-000000000002"
}
```

## Flattened keys

In application code you can model scopes as a nested object:

```typescript
{
  eventType: 'GradeAssigned',
  gradeAssignedId: '018f2d5e-1004-7000-8000-000000000004',
  grade: 'B+',
  scopes: {
    studentEnrolledInCourseId: '018f2d5e-1003-7000-8000-000000000003',
    studentRegisteredId: '018f2d5e-1001-7000-8000-000000000001',
    coursePublishedId: '018f2d5e-1002-7000-8000-000000000002',
  },
}
```

Flatten it before saving if you want to query or index `scopes.coursePublishedId`, then unflatten it after reads. Orisun matches JSON keys exactly; `scopes.coursePublishedId` is a key name, not an implicit nested path.

## Query from any scope

To rebuild everything in a published course scope, read the root event and every event carrying the course scope:

```json
{
  "criteria": [
    {
      "tags": [
        {"key": "eventType", "value": "CoursePublished"},
        {"key": "coursePublishedId", "value": "018f2d5e-1002-7000-8000-000000000002"}
      ]
    },
    {
      "tags": [
        {"key": "scopes.coursePublishedId", "value": "018f2d5e-1002-7000-8000-000000000002"}
      ]
    }
  ]
}
```

To focus on the enrollment, change only the root and scope key:

```json
{
  "criteria": [
    {
      "tags": [
        {"key": "eventType", "value": "StudentEnrolledInCourse"},
        {"key": "studentEnrolledInCourseId", "value": "018f2d5e-1003-7000-8000-000000000003"}
      ]
    },
    {
      "tags": [
        {"key": "scopes.studentEnrolledInCourseId", "value": "018f2d5e-1003-7000-8000-000000000003"}
      ]
    }
  ]
}
```

Criteria entries are ORed together. Tags inside one criterion are ANDed together.

Including `eventType` on root-event criteria keeps query shapes specific and aligns with partial indexes. Scope-only criteria are useful when you intentionally want every event inside that scope.

## Use scopes with CCC

Scopes are a natural fit for [Command Context Consistency](../concepts/command-context-consistency). A command that assigns a grade can read the enrollment scope, decide whether grading is allowed, then save `GradeAssigned` only if that same scoped context is unchanged.

```json
{
  "boundary": "courses",
  "query": {
    "expected_position": {
      "commit_position": 100,
      "prepare_position": 7
    },
    "subsetQuery": {
      "criteria": [
        {
          "tags": [
            {"key": "eventType", "value": "StudentEnrolledInCourse"},
            {"key": "studentEnrolledInCourseId", "value": "018f2d5e-1003-7000-8000-000000000003"}
          ]
        },
        {
          "tags": [
            {"key": "scopes.studentEnrolledInCourseId", "value": "018f2d5e-1003-7000-8000-000000000003"}
          ]
        }
      ]
    }
  },
  "events": [
    {
      "event_id": "018f2d5e-1004-7000-8000-000000000004",
      "event_type": "GradeAssigned",
      "data": "{\"gradeAssignedId\":\"018f2d5e-1004-7000-8000-000000000004\",\"grade\":\"B+\",\"assignedAt\":\"2026-06-22T10:00:00Z\",\"scopes.studentEnrolledInCourseId\":\"018f2d5e-1003-7000-8000-000000000003\",\"scopes.studentRegisteredId\":\"018f2d5e-1001-7000-8000-000000000001\",\"scopes.coursePublishedId\":\"018f2d5e-1002-7000-8000-000000000002\"}",
      "metadata": "{}"
    }
  ]
}
```

Use all criteria that the command model actually read. If grading rules depend on the course, student status, prior grades, or grade-discussion state, include the relevant root and scope criteria in the same `subsetQuery`.

## Index scope keys

Create indexes for scope keys used by command contexts or high-volume projections:

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/CreateIndex <<EOF
{
  "boundary": "courses",
  "name": "course_scope",
  "fields": [
    {"json_key": "scopes.coursePublishedId", "value_type": "TEXT"}
  ]
}
EOF
```

For high-volume event categories, prefer partial indexes with `eventType` conditions:

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/CreateIndex <<EOF
{
  "boundary": "courses",
  "name": "grade_assigned_enrollment_scope",
  "fields": [
    {"json_key": "scopes.studentEnrolledInCourseId", "value_type": "TEXT"}
  ],
  "conditions": [
    {"key": "eventType", "operator": "=", "value": "GradeAssigned"}
  ],
  "condition_combinator": "AND"
}
EOF
```

## Guidelines

- Put the creating event's id on the creating event itself, such as `coursePublishedId`.
- Put backlinks to earlier events under queryable scope keys, such as `scopes.coursePublishedId`.
- Carry inherited scopes when outer-scope queries matter; otherwise the reader would need graph traversal.
- Keep `metadata` for tracing, request source, and operational context; put domain scopes in `data`.
- Index scope keys that appear in CCC `subsetQuery` values or replay filters.
