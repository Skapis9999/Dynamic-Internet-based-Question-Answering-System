"""Run the thesis' 49-question benchmark against the live system and grade the answers.

Usage (from the repo root, with the cluster running):
    python eval/run_eval.py                 # all questions
    python eval/run_eval.py --ids 41 42 43  # a subset
Standard library only. Writes eval/results/<timestamp>.md and .json.

Grading: qwen3 (via Ollama on the host) compares each final answer to the reference answer.
Open questions without a reference are graded on whether the answer is reasonable and
supported. It's an automatic, approximate judge, the same model that wrote the answers,
so treat the numbers as indicative, and spot-check them in the report.
"""
import argparse
import json
import statistics
import time
import urllib.request
from datetime import datetime
from pathlib import Path

API = "http://127.0.0.1:8000"   # not "localhost": Python tries IPv6 ::1 first (+2s per request on Windows)
OLLAMA = "http://127.0.0.1:11434"
JUDGE_MODEL = "qwen3:8b"
HERE = Path(__file__).parent

JUDGE_PROMPT = """You grade answers of a web question-answering system. Today's date is {today}.
The reference answer is authoritative and up to date: trust it over your own (possibly outdated)
knowledge, e.g. for recent elections or events.
Question: {q}
Reference answer: {ref}
System answer: {a}

Grade the system answer:
- "correct": answers the question correctly (consistent with the reference, if one is given; extra detail is fine)
- "partial": partly correct, incomplete, or hedged between right and wrong
- "incorrect": wrong or contradicts the reference
- "no_answer": says it cannot answer / sources don't contain it
For questions without a reference, grade "correct" if the answer is a reasonable, relevant answer."""

JUDGE_SCHEMA = {"type": "object", "properties": {
    "verdict": {"type": "string", "enum": ["correct", "partial", "incorrect", "no_answer"]},
    "reason": {"type": "string"}}, "required": ["verdict", "reason"]}


def post_json(url: str, body: dict, timeout: float = 30) -> dict:
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def ask(question: str, timeout: float = 150) -> dict:
    t0 = time.time()
    qid = post_json(f"{API}/api/ask", {"question": question})["qid"]
    res = {"qid": qid, "pages_ok": 0, "pages_failed": 0, "pages_cached": 0, "fast": None, "final": None}
    with urllib.request.urlopen(f"{API}/api/stream/{qid}", timeout=timeout) as r:
        for raw in r:
            line = raw.decode("utf-8").strip()
            if not line.startswith("data:"):
                continue
            ev = json.loads(line[5:])
            if ev["type"] == "page":
                res["pages_ok" if ev.get("n_chunks") else "pages_failed"] += 1
                res["pages_cached"] += bool(ev.get("cached"))
            elif ev["type"] in ("fast", "final"):
                ev["client_seconds"] = round(time.time() - t0, 1)
                res[ev["type"]] = ev
                if ev["type"] == "final":
                    break
    return res


def judge(q: str, ref: str | None, answer: str) -> dict:
    body = {"model": JUDGE_MODEL, "stream": False, "think": False, "format": JUDGE_SCHEMA,
            # Same num_ctx as the services: a different value makes Ollama reload the model
            # (5-8 s) every time the judge and the pipeline alternate, skewing the latencies.
            "options": {"temperature": 0, "num_ctx": 4096},
            "messages": [{"role": "user", "content": JUDGE_PROMPT.format(
                today=datetime.now().date().isoformat(), q=q, ref=ref or "(none: open question)", a=answer)}]}
    out = post_json(f"{OLLAMA}/api/chat", body, timeout=120)
    return json.loads(out["message"]["content"])


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))] if xs else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ids", type=int, nargs="*")
    ap.add_argument("--pause", type=float, default=8.0, help="seconds between questions")
    args = ap.parse_args()
    questions = json.loads((HERE / "questions.json").read_text(encoding="utf-8"))
    if args.ids:
        questions = [x for x in questions if x["id"] in args.ids]

    rows = []
    for i, item in enumerate(questions):
        if i:
            time.sleep(args.pause)  # be gentle with the search engines behind SearXNG
        try:
            r = ask(item["q"])
        except Exception as e:
            r = {"error": f"{type(e).__name__}: {e}", "final": None, "fast": None}
        final = (r.get("final") or {}).get("answer", "")
        verdict = judge(item["q"], item["ref"], final) if final else {"verdict": "no_answer", "reason": r.get("error", "no final answer")}
        row = {**item, **r, "verdict": verdict["verdict"], "reason": verdict["reason"]}
        rows.append(row)
        f = r.get("final") or {}
        print(f"{item['id']:>2} {verdict['verdict']:<9} final {f.get('total_seconds', '-'):>5}s  {item['q'][:60]}", flush=True)

    # ---- summary
    def count(v, subset=rows):
        return sum(1 for x in subset if x["verdict"] == v)

    finals = [x["final"]["total_seconds"] for x in rows if x.get("final") and x["final"].get("total_seconds")]
    fasts = [x["fast"]["total_seconds"] for x in rows if x.get("fast") and x["fast"].get("total_seconds")]
    first_tok = [x["final"]["first_token_seconds"] for x in rows if x.get("final") and x["final"].get("first_token_seconds")]
    groups = {"all": rows, **{t: [x for x in rows if x["type"] == t] for t in sorted({x["type"] for x in rows})}}

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    out = HERE / "results"
    out.mkdir(exist_ok=True)
    (out / f"{stamp}.json").write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")

    md = [f"# Evaluation {stamp}", "", f"{len(rows)} questions (thesis benchmark).", "",
          "## Accuracy (graded by qwen3:8b)", "",
          "| Group | n | correct | partial | incorrect | no answer | correct % | correct+½partial % |",
          "|---|---|---|---|---|---|---|---|"]
    for name, sub in groups.items():
        n = len(sub)
        c, p = count("correct", sub), count("partial", sub)
        md.append(f"| {name} | {n} | {c} | {p} | {count('incorrect', sub)} | {count('no_answer', sub)} | "
                  f"{100 * c / n:.0f}% | {100 * (c + p / 2) / n:.0f}% |")
    md += ["", "## Latency (seconds from question submitted)", "",
           "| Metric | mean | median | p90 | max |", "|---|---|---|---|---|"]
    for name, xs in [("fast answer", fasts), ("final: first words", first_tok), ("final answer", finals)]:
        if xs:
            md.append(f"| {name} | {statistics.mean(xs):.1f} | {statistics.median(xs):.1f} | {pct(xs, 90):.1f} | {max(xs):.1f} |")
    ok = sum(x.get("pages_ok", 0) for x in rows)
    bad = sum(x.get("pages_failed", 0) for x in rows)
    cached = sum(x.get("pages_cached", 0) for x in rows)
    md += ["", f"Pages: {ok} with text, {bad} failed ({100 * bad / max(1, ok + bad):.0f}%), {cached} served from cache.",
           "", "## Answers", ""]
    for x in rows:
        f = x.get("final") or {}
        md += [f"### {x['id']}. {x['q']}  — **{x['verdict']}**",
               f"*Reference:* {x['ref'] or '—'}  ", f"*Judge:* {x['reason']}  ",
               f"*Time:* {f.get('total_seconds', '-')}s · pages {x.get('pages_ok', 0)} ok / {x.get('pages_failed', 0)} failed", "",
               "> " + (f.get("answer") or x.get("error") or "(none)").replace("\n", "\n> "), ""]
    (out / f"{stamp}.md").write_text("\n".join(md), encoding="utf-8")
    print("\n".join(md[:md.index("## Answers")]))
    print(f"\nReport: {out / (stamp + '.md')}")


if __name__ == "__main__":
    main()
