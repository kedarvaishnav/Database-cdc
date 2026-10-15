# Stack Overflow revision sample attribution

`stackoverflow_revisions.jsonl` contains a small sample of public edit
histories for these Stack Overflow answers:

- [How to convert byte size into human readable format in Java?](https://stackoverflow.com/a/3758880), post ID `3758880`.
- [Answer 4888400](https://stackoverflow.com/a/4888400), post ID `4888400`.

They were retrieved from the Stack Exchange API's
[`posts/{ids}/revisions` endpoint](https://api.stackexchange.com/docs), which
returns [revision objects](https://api.stackexchange.com/docs/types/revision).

The original content and per-revision author details are retained in each
event's `payload`. The source's `content_license` value is retained per event;
this capture contains revisions published under CC BY-SA 2.5, 3.0, and 4.0.
Author names and
profile links are included in `author_display_name` and `author_url`.

The importer modified the source by wrapping each revision in this project's
CDC JSONL envelope, mapping revision 1 to `INSERT` and later revisions to
`UPDATE`, and shuffling event arrival order while leaving source timestamps
unchanged. This demonstrates replay and latest-state reduction; Stack
Overflow's public revisions API does not expose every kind of database
operation, so this sample should not be described as a raw database CDC feed.

To refresh or regenerate the sample in WSL:

```bash
python3 src/fetch_stackoverflow_revisions.py
```

The source data is user-contributed content. Preserve this attribution and
the per-row license information when redistributing the sample or derivatives.
