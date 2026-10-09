# What we found

Two weeks of deliberately breaking Striim pipelines and watching what the
platform told us — and, more often, what it didn't.

Written for anyone who depends on this data. The engineering detail lives in
a separate document.

---

## The one-line version

> **Striim reliably tells you what it did. Nothing tells you what actually
> ended up in your database.**

Those sound like the same thing. They aren't, and the gap between them is
where the risk sits.

---

## 1. Your data can silently double

A pipeline stops. Someone restarts it — the obvious thing to do, and usually
correct.

For some pipelines, restarting means starting the file over from the
beginning. Every row that was already delivered gets delivered again. Your
warehouse now holds two of everything.

**Nothing announces this.** No alert. No error. No failed rows. The status
still reads healthy and the record counts still look sensible.

**What it costs:** revenue overstated, customer counts inflated, every report
built on that table quietly wrong — until someone notices a number that
cannot be right, usually weeks later.

---

## 2. Your data can silently disappear

We deleted the destination file while the pipeline was running, to see what
would happen.

Nothing happened. No alert, no error, no change of status. The pipeline
carried on reporting that it was fine, because from where it stands, it was —
it had done its work. Nobody had asked whether the work had landed.

Striim watches the side it reads from. Nothing watches the side it writes to.

**What it costs:** the same as above, in the other direction. Reports built on
data that isn't there.

---

## 3. The numbers that look right can be wrong

The obvious defence is to check the counts. If the same number went in as came
out, everything must be fine.

We saw a pipeline report **9,900 records in and 9,900 out** — reconciling
perfectly — while the destination actually held around **1,900**.

The counters weren't lying. 9,900 records really had been written over the
course of the day. But rows had been removed since, and nothing in Striim
counts what is *currently there*.

**A matched in-and-out count is not proof that your data is correct.** It only
proves the pipeline did some work.

---

## 4. Rows can be rejected and thrown away, with no warning

When your destination refuses a record — a duplicate, a value too long, a
missing field — the record goes into a holding area and the pipeline carries
on.

That is the right design. A single bad record shouldn't stop everything.

But nothing tells you it happened. We watched a record get rejected and sit
there for six minutes: present in the source, absent from the destination,
no alert of any kind. On a well-designed destination this loss is
**permanent** — the record is never retried.

**What it costs:** a slow, invisible leak. Not a failure you can point at, but
a destination that drifts further from the truth every week.

---

---

## 6. When something does break, the alert doesn't say why

This is the one everybody already knows.

The message is "pipeline halted". That's the whole message. But four
completely different problems produce it, and each needs a different fix:

| What happened | What to do |
|---|---|
| The destination database was rebooted | wait, then resume |
| A password expired | someone has to renew it |
| One bad record the destination refuses | restarting will fail again |
| The source file was deleted | restore it first |

So somebody wakes up, finds a laptop, connects, and spends **20 to 40 minutes**
working out which. The fix itself is usually one command — once you know which
command.

And a good share of the time, the answer turns out to be that nothing was
actually wrong.

---

## What these have in common

Every one of them is a case where **something goes wrong and no alarm exists.**

Not "the alarm was missed" or "nobody was watching" — there is no alarm to
miss. The platform is reporting accurately on what it can see. The problem is
what falls outside that.

| | |
|---|---|
| Wrong data, no alert | 1, 2, 3, 4, 7 |
| Looks healthy, isn't | 2, 5, 7 |
| Alert exists, but says nothing useful | 6 |

---

## Why this needs an agent rather than more alerts

More alerts would help with problem 6 and nothing else. The other six aren't
missing notifications — they're questions nobody is asking:

- *Is what's in the destination actually what should be there?*
- *Has this pipeline's position moved since the last time we looked?*
- *Were any records refused, and what happened to them?*
- *Would restarting this fix it, or make it worse?*

Those need something that investigates, compares both ends, and reasons about
what it finds — not a threshold that fires when a number crosses a line.

That's what we built.

---

*Every finding here was reproduced on Striim's own developer environment. Nothing is taken from documentation, and
each one can be demonstrated on demand.*
