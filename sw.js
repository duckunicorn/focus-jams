// Focus Jams service worker: only used to show notifications (bus alerts). It doesn't cache anything,
// so updates to the site always load fresh.
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", e => e.waitUntil(self.clients.claim()));
self.addEventListener("notificationclick", e => {
  e.notification.close();
  e.waitUntil(self.clients.matchAll({type: "window", includeUncontrolled: true}).then(list => {
    for (const c of list) if ("focus" in c) return c.focus();
    if (self.clients.openWindow) return self.clients.openWindow(e.notification.data && e.notification.data.url || "./");
  }));
});
