// Admin CSRF helper.
//
// Wraps window.fetch so every state-changing same-origin call from an admin
// page carries the `X-CSRF-Token` header from the `admin_csrf` cookie. Pair
// with the double-submit-cookie check in middleware/admin_middleware.py.
//
// Stamps on:
//   - any path that starts with /admin (apex domain, e.g. api.approachnote.com)
//   - any same-origin path on the admin subdomain (admin.approachnote.com,
//     where the WSGI subdomain middleware strips `/admin` from public URLs
//     before they reach the browser)
//
// Stays out of cross-origin requests so the token can't leak to third-party
// APIs an admin page might call.

(function () {
    if (window.__adminFetchPatched) return;
    window.__adminFetchPatched = true;

    function readCsrf() {
        var m = document.cookie.match(/(?:^|; )admin_csrf=([^;]*)/);
        return m ? decodeURIComponent(m[1]) : '';
    }

    function shouldStampCsrf(url) {
        if (typeof url !== 'string') return false;
        // Explicit /admin paths always count, even on apex.
        if (url.startsWith('/admin')) return true;
        // Otherwise resolve against current origin and stamp iff
        // same-origin. On the admin subdomain that's the whole site;
        // on apex this is effectively a no-op (admin pages here only
        // fetch /admin/* URLs, already covered above).
        try {
            var u = new URL(url, window.location.origin);
            return u.origin === window.location.origin;
        } catch (_e) {
            return false;
        }
    }

    var UNSAFE = { POST: 1, PUT: 1, PATCH: 1, DELETE: 1 };

    var origFetch = window.fetch.bind(window);
    window.fetch = function (input, init) {
        init = init || {};
        var url = (typeof input === 'string') ? input : (input && input.url) || '';
        var method = (init.method || (input && input.method) || 'GET').toUpperCase();

        if (UNSAFE[method] && shouldStampCsrf(url)) {
            var headers = new Headers(init.headers || (input && input.headers) || undefined);
            if (!headers.has('X-CSRF-Token')) {
                headers.set('X-CSRF-Token', readCsrf());
            }
            init.headers = headers;
        }
        return origFetch(input, init);
    };
})();
