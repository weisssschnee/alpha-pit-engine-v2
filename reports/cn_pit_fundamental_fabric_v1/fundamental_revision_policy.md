# CN PIT Fundamental Revision Policy v1

The local statement archive is a current provider snapshot, not a revision tape. Each
statement/report-period normally has one row. `NOTICE_DATE` records the initial notice
date and `UPDATE_DATE` records the source's last update date, but the superseded values
are absent.

## Safe rule

- Never use `REPORT_DATE` as observable time.
- Require `NOTICE_DATE`, `UPDATE_DATE`, and `REPORT_DATE` for statement rows.
- Treat the current value as eligible only from the next declared trading session after
  `max(NOTICE_DATE, UPDATE_DATE)`.
- If `UPDATE_DATE > NOTICE_DATE`, do not reconstruct or impute the initial value. Before
  the update, the value is unknown. The row may supply a level after the safe time but
  cannot claim a measured revision delta or an initial-disclosure event.
- If any required clock is missing, exclude the row as `PIT_CONTRACT_UNRESOLVED`.
- Date-only announcements mature at the next session open (09:30 Asia/Shanghai), never
  on the same trading date.

## TTM and temporal changes

TTM uses only components whose current versions were already observable at the query
time. Annual rows equal the disclosed annual value. Regular Q1/H1/Q3 values use prior
annual plus current YTD minus the prior-year comparable YTD. Irregular periods or
missing components remain unknown. Generic QoQ/YoY/slope/persistence/acceleration must
operate on the as-of disclosed sequence; they may not read a later version.

## Dataset-specific isolation

`zygc_em` contains only report dates, so its values and event pulses remain
`PIT_CONTRACT_UNRESOLVED`. Holder rows missing `公告日期` are excluded individually;
other holder disclosures can proceed. No unresolved family is replaced by a guessed
lag or a current snapshot.
