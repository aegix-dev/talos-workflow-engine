# Finding the first external user

This is a maintainer-facing planning doc. The 1.0 gate in the README is
"at least one publicly-deployed production user willing to be a
reference" — this file lists concrete venues, framings, and what
"counts" so that pursuit doesn't drift into vanity metrics.

## What "one user" means

Not stars. Not retweets. Not comments saying "cool project."

A real first user is:

* Running the engine in something they call production (paid users,
  internal tooling that several people depend on, a CI job that
  matters).
* Running it for at least 30 days without a maintainer-pushed bugfix
  release in between (or pushing through bugfix releases and still
  staying on it).
* Willing to be named on the README, or at minimum quoted as a
  reference when other prospects ask "who else uses this?"

If you can't get all three from the same org, two-of-three from one
plus a public anecdote ("we evaluated this for X, here's why we picked
it / didn't") from another is enough to clear the 1.0 gate. The point
of the gate is to have evidence the public API survives contact with
non-author code.

## Venues, ranked by signal-to-effort

The order below is "where to spend the first hour," not "where the
biggest audience is." Big audiences with no Rust + workflow overlap
produce noise.

### Tier 1 — high-signal, low-effort

1. **This Week in Rust — crate of the week / library news.** Submit
   via PR to `this-week-in-rust/this-week-in-rust`. The crate-of-the-
   week submission is a few sentences; the bar is "interesting and
   non-trivial." A 0.2.0 release with the typed-error / cancellation
   /  sub-workflow story qualifies. Audience is Rust-fluent; the
   click-throughs are people who at least skim crate docs.
2. **Rust Users Forum (users.rust-lang.org).** A "Show & tell" thread
   with a 200-word post + links to README, the production-stack guide,
   and the announcement. Slower than Reddit but the responses tend to
   be from people who actually read the code. Stays linkable for
   months.
3. **/r/rust weekly "What's everyone working on" thread.** Posted
   every Wednesday-ish. Low pressure (it's a comment, not a post),
   and the audience is people deliberately looking for Rust projects.
   Lead with what's *new since 0.1*, not the elevator pitch.

### Tier 2 — bigger reach, more variance

4. **/r/rust top-level post.** Higher visibility than the weekly
   thread. Lead with the 0.2.0 announcement; link the production-stack
   guide and the announcement post in `docs/announcements/v0.2.0.md`.
   Read the [/r/rust posting guidelines](https://www.reddit.com/r/rust/wiki/posting)
   first — they have explicit norms about self-promotion frequency
   and "release" vs "show" framing.
5. **Lobste.rs (rust + practices tags).** Smaller audience than HN
   but higher density of practitioners who'll critique the API
   directly. Submit the announcement post, not the README. Be ready
   to answer "vs. Temporal?" in the first comment.
6. **Hacker News — Show HN.** High variance. Use the title format
   `Show HN: Talos — Rust workflow engine for WASM modules and AI
   agents`. The discussion will be Temporal/Airflow comparisons; have
   the README's comparison section memorized. Avoid posting on
   weekends or during US-Asia overlap dead zones (~03:00-08:00 UTC).

### Tier 3 — adjacent communities

7. **Tokio Discord (#showcase).** Async-Rust users overlap heavily
   with the target audience. Drop a one-paragraph link with the
   "what's new in 0.2" framing.
8. **AI engineering communities.** The agent primitives (`AgentLoop`,
   `ReActLoop`, `Judge`, `Ensemble`, `ReflectiveRetry`) are unusual
   among workflow engines. Worth posting in:
   * **LangChain / LangGraph Discord** — they'll ask "why not just
     use LangGraph?" Have the WASM-sandboxing + signed-wire-format
     answer ready. Most of them won't switch; the goal is finding
     the one team that needs the sandbox.
   * **Anthropic developer Discord / forum** — same shape, smaller
     audience, higher Rust-curiosity density.
   * **/r/LocalLLaMA** — long shot, but a fraction of that audience
     runs local models behind real production traffic and would
     benefit from a sandboxed orchestrator.
9. **Rust workflow / orchestration discussion threads.** When someone
   asks "is there a Rust equivalent of Temporal / Airflow / Inngest?"
   on Reddit, Stack Overflow, or HN, *answer the question
   honestly*. Include the comparison from the README — what this is
   and isn't. A truthful "this might fit your use case if X, but
   probably not if Y" earns more trust than the post itself.

### Tier 4 — long-lead

10. **Conference talks.** RustConf, EuroRust, Rust Nation — CFPs open
    months in advance. A 25-minute talk on "we wrote a workflow engine
    in Rust because Temporal didn't fit; here's what we learned about
    cancellation / sub-workflows / WASM" is the right shape. Don't
    submit a 0.2-stage project; wait for 0.3+ and at least one outside
    user so the talk can include their experience.
11. **A blog post series on the personal site / project blog.** Three
    posts:
    * "Why we built a workflow engine in Rust" (motivation, comparison
      vs Temporal/Airflow).
    * "The sub-workflow recursion bug we shipped" (engineering
      narrative — the kind of post that ranks on HN months later).
    * "Cancellation in async Rust: what tokio gives you, what it
      doesn't" (extracts the pattern from the engine; useful even to
      readers who never use the library).
12. **Direct outreach.** Maintain a list of 5-10 teams/individuals
    who've publicly mentioned wanting a Rust workflow engine
    (search GitHub issues on Temporal, Inngest, Trigger.dev for "Rust"
    mentions; check `is:issue language:Rust workflow orchestration`).
    Send each a single, personalized message: "I noticed you asked
    about X in Y; we just shipped Z, would it fit your use case?"
    Do not spam. One message per person, total.

## Framing — what to lead with by audience

The same project lands differently in different rooms. Tune the lead.

| Audience | Lead with | Don't lead with |
|---|---|---|
| /r/rust, This Week in Rust | Typed errors, cancellation, sub-workflow recursion guard, fluent builder | "AI agent primitives" (eye-roll trigger) |
| Lobste.rs | Wire-format snapshot tests, `cargo deny` policy, MSRV story | "Production-ready" (skeptical audience) |
| Hacker News | Comparison vs Temporal — Rust-native, no separate server, fewer guarantees | The full feature list (people stop reading) |
| Tokio Discord | `CancellationToken` propagation through `AdapterSet` | Anything not async-Rust-specific |
| AI / agent communities | `AgentLoop` / `Judge` / `Ensemble` primitives + WASM sandbox | The Rust angle |
| Workflow-engine threads | Honest "fits if / doesn't fit if" comparison | Hard sell |

## What to ask for

Asking for "users" gets nothing. Asking for specific things people can
actually give you converts:

* **A 30-minute call to walk through your use case.** Lower bar than
  "try it." You learn what people actually need.
* **A code review of the public API on a specific PR.** Specific is
  the magic word — "look at my repo" gets ignored.
* **A bug report against a use case the README doesn't cover.** The
  most underrated form of contribution; rewards them with attention.
* **An honest "we evaluated and chose X instead because Y."** This is
  often more valuable than a user who picked you for the wrong
  reasons and will churn in a month.

## What NOT to do

* **Don't crosspost the same text to five places.** Each venue has
  its own norms; the same post will read as spam in 2/5 of them and
  flat in another 2/5.
* **Don't farm upvotes.** Vanity metrics will mislead the roadmap.
  The README's 1.0 gate is "one production user," not "1k stars."
* **Don't argue with the Temporal comparison.** People will say "but
  Temporal does X better" and they're often right. Acknowledge the
  tradeoff, point to where this project is different, and move on.
* **Don't promise features in the comments.** "Yeah, we'll add that"
  becomes a roadmap commitment you didn't think through. Say "open
  an issue with the use case and I'll think about it."
* **Don't pretend it's bigger than it is.** "Used by N companies" is
  a lie until N > 0. "Currently looking for our first production
  user" is honest and often more compelling than puffery.

## Cadence

* **0.2.0 announcement window:** ~2 weeks after the publish goes
  out. Tier 1 in week 1, Tier 2 in week 2, Tier 3 spaced over the
  month after.
* **Subsequent releases:** only Tier 1 (TWiR + Users Forum +
  /r/rust weekly thread) unless the release is materially newsworthy
  (new wire-format addition, major API change, security advisory).
  Re-running Tier 2 every patch release burns goodwill fast.
* **Stop counting once you have a user.** Switch to talking to
  *that user* about what they need next. The marginal external user
  matters less than the depth of the relationship with the first.

## Tracking

Maintain a simple text file at `~/.talos-growth.md` (not in this
repo — it'll have notes on individuals):

```
2026-04-22 — TWiR submission PR: <url>
2026-04-23 — /r/rust weekly thread comment: <url>
2026-04-24 — Lobste.rs submission: <url>
2026-04-26 — User Forum thread: <url>, 4 replies, 1 follow-up DM from <handle>
2026-04-30 — DM thread w/ <handle>: evaluated for use case X, picked Inngest, gave detailed
             feedback on the Wait API (link to issue: ...)
```

The point is to remember who said what, not to produce metrics. If
the file ever grows past one screen, you're tracking the wrong things.
