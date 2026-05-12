import type { ReactNode } from "react";

export function TerminalSection(props: { title: string; meta?: string; children: ReactNode }) {
  return (
    <section className="terminal-section">
      <div className="terminal-section-head">
        <span>{props.title}</span>
        {props.meta ? <em>{props.meta}</em> : null}
      </div>
      {props.children}
    </section>
  );
}
