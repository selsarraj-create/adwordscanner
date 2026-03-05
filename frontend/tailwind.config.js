/** @type {import('tailwindcss').Config} */
export default {
    content: [
        "./index.html",
        "./src/**/*.{js,ts,jsx,tsx}",
    ],
    theme: {
        extend: {
            colors: {
                'pastel-bg': '#121212',     // Rich Near-Black (OLED friendly)
                'pastel-card': '#1E1E1E',   // Elevated Gray
                'pastel-accent': '#FF2E63', // Glowing Coral/Pink (High Brightness for Dark Mode)
                'pastel-text': '#FFFFFF',   // Pure White
                'pastel-muted': '#A0A0A0',  // Silver/Light Gray
            },
            fontFamily: {
                sans: ['Inter', 'sans-serif'],
            },
            animation: {
                'scan': 'scan 4s linear infinite',
            },
            keyframes: {
                scan: {
                    '0%': { top: '0%' },
                    '100%': { top: '100%' },
                }
            }
        },
    },
    plugins: [],
}
