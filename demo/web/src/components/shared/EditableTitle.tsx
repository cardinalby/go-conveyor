import { useState } from "react";

interface Props {
  /** The name currently on display — the pipeline's own custom name, or the positional default (see
   * ../../pipeline/names) when none is set. Editing always starts from this text. */
  value: string;
  /** Commits a new custom name, trimmed. Called with "" when the edit is blank/whitespace-only, which is what
   * clears a custom name back to the positional default — see ../../pipeline/resolve's `n.name || positionalName`
   * fallback. Not called at all if the trimmed edit is unchanged from value. */
  onCommit: (name: string) => void;
  className?: string;
}

/** A node/lane title that becomes a text input on click, committing on blur or Enter and reverting on Escape — the
 * only way in the demo to give a node/lane a name other than go-conveyor's own positional default. "nodrag nopan"
 * keeps React Flow's own drag/pan gesture listeners from stealing the click — see index.css's node section. */
export function EditableTitle({ value, onCommit, className }: Props) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(value);

  function commit() {
    setEditing(false);
    const trimmed = draft.trim();
    if (trimmed !== value) onCommit(trimmed);
  }

  if (editing) {
    return (
      <input
        autoFocus
        className={`node-name-input nodrag nopan${className ? ` ${className}` : ""}`}
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onClick={(e) => e.stopPropagation()}
        onBlur={commit}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            e.currentTarget.blur(); // commits via onBlur
          } else if (e.key === "Escape") {
            setEditing(false); // no commit — the draft is discarded
          }
        }}
      />
    );
  }

  return (
    <div
      className={`node-name nodrag nopan${className ? ` ${className}` : ""}`}
      title="Click to rename"
      onClick={(e) => {
        e.stopPropagation();
        setDraft(value);
        setEditing(true);
      }}
    >
      {value}
    </div>
  );
}
