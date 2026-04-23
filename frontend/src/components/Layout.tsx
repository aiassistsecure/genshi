import { Link, NavLink, Outlet, useLocation } from "react-router-dom";
import { useEffect, useState } from "react";
import { api } from "../api";

export default function Layout() {
  const [healthy, setHealthy] = useState<{ has_netrows_key: boolean; has_aiassist_key: boolean } | null>(null);
  const [open, setOpen] = useState(false);
  const loc = useLocation();
  useEffect(() => { api.health().then(setHealthy).catch(() => setHealthy(null)); }, []);
  useEffect(() => { setOpen(false); }, [loc.pathname]);

  const navCls = ({ isActive }: { isActive: boolean }) =>
    `block rounded-md px-3 py-2 text-sm font-medium transition-colors ${
      isActive ? "bg-ink-900 text-white" : "text-ink-700 hover:bg-ink-100"
    }`;

  const Brand = () => (
    <Link to="/" className="flex items-center gap-2 px-2">
      <div className="w-8 h-8 rounded-md bg-ink-900 text-white grid place-items-center font-bold">玄</div>
      <div>
        <div className="font-semibold tracking-tight leading-tight">Genshi</div>
        <div className="text-[11px] text-ink-500 -mt-0.5">Generates Sheets</div>
      </div>
    </Link>
  );

  const NavLinks = () => (
    <nav className="flex flex-col gap-1">
      <NavLink to="/" end className={navCls}>Sheets</NavLink>
      <NavLink to="/new" className={navCls}>New sheet</NavLink>
      <NavLink to="/templates" className={navCls}>Templates</NavLink>
      <NavLink to="/settings" className={navCls}>Settings</NavLink>
    </nav>
  );

  const HealthDots = () => (
    <div className="text-[11px] text-ink-500 space-y-1">
      <div className="flex items-center gap-1.5">
        <span className={`w-1.5 h-1.5 rounded-full ${healthy?.has_netrows_key ? "bg-accent-500" : "bg-ink-300"}`} />
        Netrows {healthy?.has_netrows_key ? "connected" : "missing"}
      </div>
      <div className="flex items-center gap-1.5">
        <span className={`w-1.5 h-1.5 rounded-full ${healthy?.has_aiassist_key ? "bg-accent-500" : "bg-ink-300"}`} />
        AiAssist {healthy?.has_aiassist_key ? "connected" : "missing"}
      </div>
    </div>
  );

  return (
    <div className="h-[100dvh] flex flex-col md:flex-row overflow-hidden">
      {/* Mobile top bar */}
      <header className="md:hidden flex items-center justify-between border-b border-ink-200 bg-white px-3 py-2 shrink-0">
        <Brand />
        <button
          aria-label="Menu"
          onClick={() => setOpen((v) => !v)}
          className="p-2 rounded-md hover:bg-ink-100"
        >
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            {open
              ? <><path d="M6 6l12 12" /><path d="M18 6L6 18" /></>
              : <><path d="M4 7h16" /><path d="M4 12h16" /><path d="M4 17h16" /></>}
          </svg>
        </button>
      </header>

      {/* Mobile drawer */}
      {open && (
        <>
          <div className="md:hidden fixed inset-0 bg-black/30 z-40" onClick={() => setOpen(false)} />
          <aside className="md:hidden fixed top-0 right-0 bottom-0 w-64 bg-white z-50 px-4 py-5 flex flex-col gap-5 shadow-xl">
            <Brand />
            <NavLinks />
            <div className="mt-auto pt-4 border-t border-ink-100"><HealthDots /></div>
          </aside>
        </>
      )}

      {/* Desktop sidebar */}
      <aside className="hidden md:flex w-60 shrink-0 border-r border-ink-200 bg-white px-4 py-6 flex-col">
        <div className="mb-8"><Brand /></div>
        <NavLinks />
        <div className="mt-auto pt-4 border-t border-ink-100"><HealthDots /></div>
      </aside>

      <main className="flex-1 min-w-0 min-h-0 overflow-hidden flex flex-col">
        <Outlet />
      </main>
    </div>
  );
}
