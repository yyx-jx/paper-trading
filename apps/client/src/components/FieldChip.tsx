export type FieldChipTone = "positive" | "negative" | "neutral" | "warning" | "info";

export function FieldChip(props: { label: string; tone?: FieldChipTone }) {
  return <span className={`field-chip tone-${props.tone ?? "neutral"}`}>{props.label}</span>;
}
