import { useEffect } from 'react';
import { useLocation } from 'react-router-dom';

const MetaPixel = () => {
    const location = useLocation();
    const PIXEL_ID = import.meta.env.VITE_META_PIXEL_ID;

    useEffect(() => {
        if (!PIXEL_ID) {
            console.warn("Meta Pixel ID not found in VITE_META_PIXEL_ID");
            return;
        }

        // Initialize Meta Pixel
        !(function (f, b, e, v, n, t, s) {
            if (f.fbq) return;
            n = f.fbq = function () {
                n.callMethod
                    ? n.callMethod.apply(n, arguments)
                    : n.queue.push(arguments);
            };
            if (!f._fbq) f._fbq = n;
            n.push = n;
            n.loaded = !0;
            n.version = '2.0';
            n.queue = [];
            t = b.createElement(e);
            t.async = !0;
            t.src = v;
            s = b.getElementsByTagName(e)[0];
            s.parentNode.insertBefore(t, s);
        })(
            window,
            document,
            'script',
            'https://connect.facebook.net/en_US/fbevents.js'
        );

        window.fbq('init', PIXEL_ID);
        // Note: We don't track initial pageview here because the route change effect handles it
        // actually for the first load we might want it.
        // But let's rely on the location effect below.

    }, [PIXEL_ID]);

    // Track PageView on route change
    useEffect(() => {
        if (PIXEL_ID && window.fbq) {
            window.fbq('track', 'PageView');
        }
    }, [location, PIXEL_ID]);

    return null;
};

export default MetaPixel;
