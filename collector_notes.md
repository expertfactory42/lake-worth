# Collector Performance & Throttle Notes

## Run: 2026-03-19

### Results
- **490 new entries** across 5 productive sessions (Session 6 got zero)
- Coverage advanced from **1915-11-16 to 1916-08-07**
- Total database: 1,426 references (1,280 no-article entries, 169 articles)
- Date range in DB: 1909-04-03 to 1916-08-07
- Still need: 1916-08 through 1925-12

### Timing
- Start: 08:50 (Session 1)
- Session 1: 08:50–09:01 (100 entries, 11 min)
- Session 2: 09:01–09:12 (100 entries, 11 min)
- Session 3: 09:12–09:22 (100 entries, 10 min)
- Session 4: 09:22–09:33 (100 entries, 11 min)
- Session 5: 09:33–09:43 (90 entries, then 504 Gateway Timeout crashed browser)
- Session 6: 09:44–09:45 (0 entries — throttled, search returned empty)
- **Total: 490 entries in ~53 minutes**

### Throttle Behavior
- newspapers.com uses Cloudflare protection
- Throttle kicked in around **50 minutes / ~500 page loads**
- Not a hard 429 error — instead serves empty search results (soft block)
- 504 Gateway Timeout appeared just before full throttle
- Rate was ~9-10 entries/minute (one page load every 6-7 seconds)
- Immediate retry after throttle returns zero results
- Previous observation: download rate limit resets hourly (may apply to search too)

### Recommendations for Future Runs
- Wait at least 1 hour between runs
- Consider increasing ACTION_DELAY from 2 to 4-5 seconds
- Consider lowering RESTART_EVERY from 100 to 50 for shorter sessions
- Add randomized delays (3-8 sec) between page visits to appear more human
- Monitor for 504 errors as early warning of impending throttle
- **~500 page views per hour limit** — resets on the hour from when the run started
- After exactly 1 hour from start of the 490-entry run, the throttle lifted and collection resumed successfully
- However, second run got 1,264 entries over 2h15m with no throttle — limit may be 500 in a burst, not a hard hourly cap. Spread over longer time with browser restarts may avoid it.

## Run: 2026-03-19 (second run)

### Results
- **1,264 new entries** across 13 sessions
- Coverage advanced from **1916-08-07 to 1917-08-08**
- Session 13 ended naturally with 64 entries (all results collected)
- No throttle encountered despite 1,264 page loads over 2h15m

### Timing
- Start: 09:55, End: 12:10 (~2h15m)
- ~10 min per 100-entry session, consistent with first run
- Total DB after: ~2,690 references
