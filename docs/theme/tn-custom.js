'use strict';

// Telcoin Network theme behaviors. Three independent, null-guarded modules:
//   1. header theme switch  — relays clicks to the hidden stock theme-list
//      buttons so book.js keeps ownership of theme state and localStorage;
//   2. search extras        — Cmd/Ctrl-K shortcut, platform-aware kbd label;
//   3. right rail           — "On this page" TOC with scroll-spy (>=1280px).

(function tnThemeSwitch() {
    var switchEl = document.querySelector('.tn-theme-switch');
    if (!switchEl) {
        return;
    }

    function savedTheme() {
        try {
            return localStorage.getItem('mdbook-theme');
        } catch (e) {
            return null;
        }
    }

    function renderSwitch() {
        var saved = savedTheme();
        var choice = saved === 'light' || saved === 'navy' ? saved : 'default_theme';
        var buttons = switchEl.querySelectorAll('.tn-theme-choice');
        Array.prototype.forEach.call(buttons, function(btn) {
            var checked = btn.getAttribute('data-theme-choice') === choice;
            btn.setAttribute('aria-checked', checked ? 'true' : 'false');
        });
    }

    switchEl.addEventListener('click', function(e) {
        var btn = e.target.closest('.tn-theme-choice');
        if (!btn) {
            return;
        }
        // The stock buttons stay in the DOM (display:none); a synthetic click
        // reuses book.js theme switching, storage and highlight-css handling.
        var stock = document.getElementById(
            'mdbook-theme-' + btn.getAttribute('data-theme-choice'));
        if (stock) {
            stock.click();
        }
        renderSwitch();
    });

    // Keep the switch in sync if another tab changes the stored theme.
    window.addEventListener('storage', function(e) {
        if (e.key === 'mdbook-theme') {
            renderSwitch();
        }
    });

    renderSwitch();
})();

(function tnSearchExtras() {
    var toggle = document.getElementById('mdbook-search-toggle');

    // Platform-aware shortcut hint in the search pill.
    var kbd = document.querySelector('.tn-search-kbd');
    var isApple = /Mac|iPhone|iPad|iPod/.test(navigator.platform || '');
    if (kbd && !isApple) {
        kbd.textContent = 'Ctrl K';
    }

    var searchbar = document.getElementById('mdbook-searchbar');
    if (searchbar) {
        searchbar.setAttribute('placeholder', 'Search…');
    }

    if (toggle) {
        document.addEventListener('keydown', function(e) {
            if ((e.metaKey || e.ctrlKey) && !e.altKey && !e.shiftKey &&
                (e.key === 'k' || e.key === 'K')) {
                e.preventDefault();
                toggle.click();
            }
        });
    }
})();

(function tnPageToc() {
    if (/print\.html$/.test(window.location.pathname)) {
        return;
    }

    var content = document.getElementById('mdbook-content');
    var wrapper = document.getElementById('mdbook-page-wrapper');
    if (!content || !wrapper) {
        return;
    }

    var headings = content.querySelectorAll('main h2[id], main h3[id]');
    if (headings.length < 2) {
        return;
    }

    var aside = document.createElement('aside');
    aside.id = 'tn-page-toc';
    aside.setAttribute('aria-label', 'On this page');

    var title = document.createElement('div');
    title.className = 'tn-toc-title';
    title.textContent = 'On this page';
    aside.appendChild(title);

    var list = document.createElement('ul');
    var links = {};
    Array.prototype.forEach.call(headings, function(h) {
        var li = document.createElement('li');
        li.className = h.tagName === 'H3' ? 'tn-toc-h3' : 'tn-toc-h2';
        var a = document.createElement('a');
        a.href = '#' + h.id;
        a.textContent = h.textContent.replace(/\s+#?\s*$/, '');
        li.appendChild(a);
        list.appendChild(li);
        links[h.id] = a;
    });
    aside.appendChild(list);
    wrapper.appendChild(aside);

    function setActive(id) {
        for (var key in links) {
            if (Object.prototype.hasOwnProperty.call(links, key)) {
                links[key].classList.toggle('active', key === id);
            }
        }
    }

    // Deterministic spy: the active entry is the last heading above the
    // activation line (header + a quarter viewport). An IntersectionObserver
    // only fires while a heading crosses its band, so it goes stale on anchor
    // jumps and programmatic scrolls.
    function currentHeading() {
        var doc = document.documentElement;
        if (window.innerHeight + window.scrollY >= doc.scrollHeight - 4) {
            return headings[headings.length - 1];
        }
        var line = 72 + window.innerHeight * 0.25;
        var current = headings[0];
        for (var i = 0; i < headings.length; i++) {
            if (headings[i].getBoundingClientRect().top <= line) {
                current = headings[i];
            } else {
                break;
            }
        }
        return current;
    }

    var ticking = false;
    function update() {
        ticking = false;
        setActive(currentHeading().id);
    }
    function requestUpdate() {
        if (!ticking) {
            ticking = true;
            (window.requestAnimationFrame || window.setTimeout)(update);
        }
    }
    window.addEventListener('scroll', requestUpdate, { passive: true });
    window.addEventListener('resize', requestUpdate, { passive: true });
    window.addEventListener('hashchange', requestUpdate);
    update();
})();

(function tnSidebarRowClick() {
    // The sidebar pill (.chapter-link-wrapper) is the full clickable row, but
    // the page link is an inner <a>; a click landing on the pill's padding or
    // the gap beside the fold toggle would otherwise do nothing. Forward those
    // to the row's page link. Clicks on the fold toggle keep their native
    // behavior. On the row for the page the reader is already on, navigating
    // again is useless — clicking anywhere on that row (the link included)
    // folds/unfolds its section instead.
    document.addEventListener('click', function (e) {
        if (!e.target || !e.target.closest) {
            return;
        }
        var wrapper = e.target.closest('.chapter-link-wrapper');
        if (!wrapper) {
            return;
        }
        var clicked = e.target.closest('a');
        if (clicked && clicked.classList.contains('chapter-fold-toggle')) {
            return;
        }
        var link = wrapper.querySelector('a[href]:not(.chapter-fold-toggle)');
        var toggle = wrapper.querySelector('a.chapter-fold-toggle');
        if (link && toggle && link.classList.contains('active')) {
            e.preventDefault();
            toggle.click();
            return;
        }
        if (clicked) {
            return;
        }
        if (link) {
            link.click();
        }
    });
})();

(function tnSidebarKeepOpen() {
    // Companion to TN-EDIT E6 in index.hbs: below 1080px the sidebar is an
    // overlay and stock closes it on every page load. Record open/closed in
    // sessionStorage whenever the user toggles it, so the inline script can
    // restore an open sidebar after in-book navigation. Session-scoped on
    // purpose: a fresh visit still starts with the overlay closed.
    var checkbox = document.getElementById('mdbook-sidebar-toggle-anchor');
    if (!checkbox) {
        return;
    }
    checkbox.addEventListener('change', function () {
        try {
            sessionStorage.setItem('tn-sidebar-open', checkbox.checked ? '1' : '0');
        } catch (e) {
            // Storage unavailable: navigation falls back to stock behavior.
        }
    });
})();

(function tnVersionPicker() {
    // Companion to TN-EDIT E7 in index.hbs. CI publishes the book from main at
    // the site root ("latest") and each v* tag under /<tag>/, plus a
    // versions.json manifest at the site root. This fills the header picker
    // from that manifest and switches versions keeping the reader on the same
    // page when the target version has it. The expected behavior is pinned in
    // docs/ux.md — update that file if this changes.
    var button = document.getElementById('tn-version-button');
    var menu = document.getElementById('tn-version-menu');
    if (!button || !menu || typeof path_to_root === 'undefined') {
        return;
    }
    var labelEl = button.querySelector('.tn-version-label');

    // Book root of this build, then split it into site root + version name.
    // A build living in a /v…/ directory is a tagged version; anything else
    // (site root, custom domain root, local mdbook serve) is "latest".
    var bookRoot = new URL(path_to_root || '.', window.location.href);
    var segments = bookRoot.pathname.split('/').filter(Boolean);
    var lastSegment = segments[segments.length - 1];
    var current = 'latest';
    var siteRoot = bookRoot;
    if (lastSegment && /^v\d/.test(lastSegment)) {
        current = lastSegment;
        siteRoot = new URL('..', bookRoot);
    }
    labelEl.textContent = current;

    function stripIndex(pathname) {
        return pathname.replace(/index\.html$/, '');
    }

    // Pending "intended page" from an earlier switch that fell back to a
    // version's start page. It survives until the reader either reaches that
    // page in another version or navigates elsewhere on their own.
    var intended = null;
    try {
        intended = JSON.parse(sessionStorage.getItem('tn-version-intended'));
    } catch (e) {
        intended = null;
    }
    if (intended && (typeof intended.page !== 'string' ||
        stripIndex(window.location.pathname) !== stripIndex(intended.landedAt || ''))) {
        intended = null;
        try {
            sessionStorage.removeItem('tn-version-intended');
        } catch (e) { }
    }

    var versions = [{ name: 'latest', root: siteRoot }];

    function switchTo(version) {
        var relPage = window.location.pathname.slice(bookRoot.pathname.length);
        var desired = intended ? intended.page : relPage;
        var target = new URL(desired, version.root);
        function fallBack() {
            // Target version lacks the page: remember it, land on that
            // version's start page, and explain there (see banner below).
            try {
                sessionStorage.setItem('tn-version-intended', JSON.stringify({
                    page: desired,
                    landedAt: version.root.pathname,
                }));
            } catch (e) { }
            window.location.href = version.root.href;
        }
        fetch(target.href, { method: 'HEAD' }).then(function (response) {
            if (response.ok) {
                try {
                    sessionStorage.removeItem('tn-version-intended');
                } catch (e) { }
                window.location.href = target.href + window.location.hash;
            } else {
                fallBack();
            }
        }).catch(fallBack);
    }

    function closeMenu() {
        menu.hidden = true;
        button.setAttribute('aria-expanded', 'false');
    }

    button.addEventListener('click', function (e) {
        e.stopPropagation();
        var opening = menu.hidden;
        menu.hidden = !opening;
        button.setAttribute('aria-expanded', opening ? 'true' : 'false');
    });
    document.addEventListener('click', function (e) {
        if (!menu.hidden && e.target.closest && !e.target.closest('.tn-version-picker')) {
            closeMenu();
        }
    });
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') {
            closeMenu();
        }
    });

    function renderMenu() {
        menu.innerHTML = '';
        versions.forEach(function (version) {
            var item = document.createElement('li');
            item.setAttribute('role', 'option');
            item.setAttribute('aria-selected', version.name === current ? 'true' : 'false');
            var choice = document.createElement('button');
            choice.type = 'button';
            choice.className = 'tn-version-option';
            choice.textContent = version.name;
            if (version.name === current) {
                choice.classList.add('current');
            }
            choice.addEventListener('click', function () {
                closeMenu();
                if (version.name !== current) {
                    switchTo(version);
                }
            });
            item.appendChild(choice);
            menu.appendChild(item);
        });
    }

    renderMenu();
    fetch(new URL('versions.json', siteRoot).href, { cache: 'no-cache' }).then(function (response) {
        return response.ok ? response.json() : null;
    }).then(function (manifest) {
        if (manifest && Array.isArray(manifest.versions)) {
            manifest.versions.forEach(function (tag) {
                if (typeof tag === 'string' && /^v\d/.test(tag)) {
                    versions.push({ name: tag, root: new URL(tag + '/', siteRoot) });
                }
            });
            renderMenu();
        }
    }).catch(function () {
        // No manifest (local build): the menu keeps only the current version.
    });

    // Content banners: a standing "old version" notice on every non-latest
    // page, and a one-off explanation when a switch fell back here because
    // the requested page does not exist in this version.
    var main = document.querySelector('#mdbook-content > main');
    if (!main) {
        return;
    }
    if (current !== 'latest') {
        var banner = document.createElement('div');
        banner.className = 'tn-version-notice tn-version-notice-old';
        var text = document.createElement('span');
        text.appendChild(document.createTextNode('You are viewing documentation for '));
        var strong = document.createElement('strong');
        strong.textContent = current;
        text.appendChild(strong);
        text.appendChild(document.createTextNode('. '));
        var latestLink = document.createElement('a');
        latestLink.href = siteRoot.href;
        latestLink.textContent = 'View the latest version';
        latestLink.addEventListener('click', function (e) {
            e.preventDefault();
            switchTo(versions[0]);
        });
        text.appendChild(latestLink);
        text.appendChild(document.createTextNode('.'));
        banner.appendChild(text);
        main.insertBefore(banner, main.firstChild);
    }
    if (intended) {
        var notice = document.createElement('div');
        notice.className = 'tn-version-notice tn-version-notice-missing';
        var noticeText = document.createElement('span');
        noticeText.appendChild(document.createTextNode('The page you were reading ('));
        var code = document.createElement('code');
        code.textContent = intended.page || 'index.html';
        noticeText.appendChild(code);
        noticeText.appendChild(document.createTextNode(') is not available in ' + current +
            ', so you are on its start page. Switching to a version that includes the page will take you back to it.'));
        notice.appendChild(noticeText);
        main.insertBefore(notice, main.firstChild);
    }
})();
