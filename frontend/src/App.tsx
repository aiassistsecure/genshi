import { Routes, Route, Navigate } from "react-router-dom";
import Layout from "./components/Layout";
import SheetList from "./pages/SheetList";
import NewSheet from "./pages/NewSheet";
import SheetView from "./pages/SheetView";
import Templates from "./pages/Templates";
import Settings from "./pages/Settings";

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<SheetList />} />
        <Route path="new" element={<NewSheet />} />
        <Route path="templates" element={<Templates />} />
        <Route path="settings" element={<Settings />} />
        <Route path="s/:id" element={<SheetView />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}
