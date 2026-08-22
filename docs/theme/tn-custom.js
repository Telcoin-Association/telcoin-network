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
