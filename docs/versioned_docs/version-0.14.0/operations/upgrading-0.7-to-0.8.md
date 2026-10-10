---
title: Upgrade from 0.7.0 to 0.8.0
description: A production runbook for migrating existing boundaries into the 0.8.0 event-backed catalog.
---

This historical bridge upgrade does not apply to the current breaking release.
Older storage formats are rejected, and the historical boundary importer is
removed. Use the [current storage upgrade policy](./upgrading-event-envelope)
for supported formats, exports, and fresh-store imports.
