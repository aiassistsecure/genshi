export type Confidence = "verified" | "high" | "medium" | "low" | "uncertain" | "invalid" | "user";

export interface Cell {
  value: any;
  source?: string;
  confidence?: Confidence;
  fetched_at?: string;
  verification?: { status: string; reason?: string; catch_all?: boolean | null };
}

export interface RowOut {
  id: string;
  position: number;
  cells: Record<string, Cell | null>;
}

export interface SheetOut {
  id: string;
  name: string;
  headers: string[];
  query: string;
  status: "draft" | "generating" | "ready" | "error";
  error: string;
  created_at: string;
  updated_at: string;
  rows: RowOut[];
}

export interface SheetSummary {
  id: string;
  name: string;
  headers: string[];
  status: SheetOut["status"];
  row_count: number;
  created_at: string;
  updated_at: string;
}

export interface Template {
  id: string;
  name: string;
  description: string;
  headers: string[];
  suggested_query: string;
  builtin: number;
}
