import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import Scanner from './components/Scanner';
import Login from './pages/Login';
import Admin from './pages/Admin';


import './index.css';

function App() {
  return (
    <Router>

      <div className="min-h-screen bg-pastel-bg text-pastel-text selection:bg-pastel-accent selection:text-black flex flex-col">
        <Routes>
          <Route path="/" element={
            <div className="flex-1 flex flex-col justify-center p-4">
              <Scanner />
            </div>
          } />
          <Route path="/login" element={<Login />} />
          <Route path="/admin" element={<Admin />} />
        </Routes>

        {/* Footer */}
        <footer className="w-full py-6 text-center text-gray-600 text-xs relative z-10 bg-black/20 backdrop-blur-sm border-t border-white/5">
          <p>© 2026 MODEL SUITABILITY ENGINE</p>
        </footer>
      </div>
    </Router>
  );
}

export default App;
