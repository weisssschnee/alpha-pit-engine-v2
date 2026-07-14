# Phase-3 qualification source review

Reviewed: 2026-07-14
External bundle: `CN_FUNDAMENTAL_QUALIFICATION_PHASE3_BUNDLE_20260714.zip`
Disposition: accepted as a non-authoritative constraint; executable heuristic superseded locally.

The bundle README and MANIFEST are retained byte-for-byte. The imported policy,
merge contract and executable builder were reviewed against the actual 1,227-row
source universe, the local semantic/PIT registry, and the implemented unified
capability identity services.

## Findings corrected

1. The draft generated `sf:<20 hex>` from a hard-coded string. This conflicted
   with the repository authority, which binds provider, source release, table
   and field under `cn_source_field_identity_v2`. The implementation now calls
   the repository `source_field_id` function and produces 1,227 unique IDs.
2. The draft inferred `MEDIUM` or `HIGH` unit certainty from field-name tokens.
   The implementation now reuses only the 42 exact local unit assertions. All
   remaining raw levels fail closed.
3. The draft could create a disclosure representation for every field carrying
   the broad `DISCLOSURE_EVENT` semantic role. That would duplicate statement
   episodes and effectively expose much of the 1,227-field universe. The
   implementation reuses the finite 147-entry canonical representation layer,
   including four deduplicated disclosure episode roots.
4. The draft labeled generic temporal changes `REVISION_AWARE_CHANGE` despite
   the source statements being current-snapshot-only. Every generated registry
   row now states `historical_revision_replay_supported=false`.
5. Metadata blocking in the draft depended mostly on a short name list. The
   implementation uses the actual `METADATA_BLOCKED` role and source blocker.
6. Financial-institution name matches are now review-only
   `INDUSTRY_SPECIFIC` rows with no route. They are not treated as asserted unit
   or applicability contracts.
7. `zygc_em` remains `PIT_UNRESOLVED`. Its single canonical registry placeholder
   is explicitly blocked and cannot be searched; it prevents absence from being
   misreported as a negative research result.
8. Source-reported YoY and internally derived YoY use the repository
   representation identity function with different source fields/types. All 19
   paired identity checks are distinct.

## Access boundary

The builder reads only:

- the versioned source-universe CSV;
- the qualification policy;
- the merge contract;
- static repository identity and representation definitions.

It does not read returns, labels, rewards, selectors, validation, holdout, 2026
data, Broad Event results, candidate results or survivor state. It does not
start search or promotion.

## Verification

- two consecutive builds produced identical artifact hashes;
- 32 targeted PIT, unified-registry and qualification tests passed;
- the 1,227 qualification states sum exactly to the source universe;
- all fail-closed states have no allowed route;
- 147 canonical representations remain below the frozen 384-root ceiling;
- 146 are search-eligible infrastructure representations and one is the blocked
  `zygc_em` placeholder.
