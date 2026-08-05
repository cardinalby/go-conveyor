import { useMemo, useState } from "react";
import { Light as SyntaxHighlighter } from "react-syntax-highlighter";
import go from "react-syntax-highlighter/dist/esm/languages/hljs/go";
import { githubGist } from "react-syntax-highlighter/dist/esm/styles/hljs";

SyntaxHighlighter.registerLanguage("go", go);

interface Props {
  open: boolean;
  code: string;
}

// Must match the highlighter's codeTagProps below so the measured width is the width it actually renders at.
const CODE_FONT = '12px ui-monospace, Menlo, Consolas, "Liberation Mono", monospace';
const BODY_PADDING_X = 24; // .code-panel-body's left+right padding
const SCROLLBAR_ALLOWANCE = 18; // room for a vertical scrollbar, so it never itself forces a horizontal one

const preStyle = { margin: 0, padding: 0, background: "transparent" };
const codeTagProps = {
  style: { fontFamily: CODE_FONT.replace(/^\d+px /, ""), fontSize: "12px", lineHeight: 1.5 },
};

let measureCtx: CanvasRenderingContext2D | null | undefined;

function longestLineWidth(code: string): number {
  if (measureCtx === undefined) measureCtx = document.createElement("canvas").getContext("2d");
  if (!measureCtx) return 0;
  measureCtx.font = CODE_FONT;
  return code.split("\n").reduce((max, line) => Math.max(max, measureCtx!.measureText(line).width), 0);
}

/** A sliding left-side drawer showing the Go code that builds a conveyor matching the current pipeline (see
 * ../codegen/generateGoCode). Always mounted so the width transition can animate it in/out; aria-hidden while
 * closed keeps it out of the tab order. Its open width tracks the widest generated line (clamped in CSS via
 * .code-panel-open) so the code never needs to scroll horizontally except in pathological cases. */
export function CodePanel({ open, code }: Props) {
  const [copied, setCopied] = useState(false);

  const panelWidth = useMemo(
    () => Math.ceil(longestLineWidth(code)) + BODY_PADDING_X + SCROLLBAR_ALLOWANCE,
    [code],
  );

  function handleCopy() {
    navigator.clipboard.writeText(code).then(() => {
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    });
  }

  return (
    <div
      className={`code-panel${open ? " code-panel-open" : ""}`}
      style={open ? { width: panelWidth } : undefined}
      aria-hidden={!open}
    >
      <div className="code-panel-inner">
        <div className="code-panel-header">
          <h2 className="code-panel-title">Generated Go code</h2>
          <button type="button" className="code-panel-button" onClick={handleCopy} tabIndex={open ? 0 : -1}>
            {copied ? "Copied!" : "Copy"}
          </button>
        </div>
        <div className="code-panel-body">
          <SyntaxHighlighter language="go" style={githubGist} customStyle={preStyle} codeTagProps={codeTagProps}>
            {code}
          </SyntaxHighlighter>
        </div>
      </div>
    </div>
  );
}
