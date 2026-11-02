// Copy buttons and the side menu toggle. The pages work without them.
document.addEventListener("click", (ev) => {
  const copy = ev.target.closest("[data-copy]");
  if (copy && navigator.clipboard) {
    navigator.clipboard.writeText(copy.dataset.copy).then(() => {
      const label = copy.textContent;
      copy.textContent = "Copied";
      setTimeout(() => (copy.textContent = label), 1200);
    });
  }
  if (ev.target.closest("[data-toggle-nav]")) {
    document.getElementById("side").classList.toggle("open");
  }
});

// Explorer: select all, the Send messages panel and its editor.
function updateEditor(ta) {
  const gutter = ta.parentElement.querySelector(".gutter");
  if (gutter) {
    const n = ta.value.split("\n").length;
    gutter.textContent = Array.from({ length: n }, (_, i) => i + 1).join("\n");
    gutter.scrollTop = ta.scrollTop;
  }
  const state = ta.closest("label").querySelector("[data-json-state]");
  const type = ta.closest("form").querySelector("[name=contentType]");
  if (state) {
    if (!type || !/json/.test(type.value) || ta.value.trim() === "") {
      state.textContent = "";
      return;
    }
    try {
      JSON.parse(ta.value);
      state.textContent = "Valid JSON";
      state.className = "jsonstate ok";
    } catch (e) {
      state.textContent = "Invalid JSON: " + e.message;
      state.className = "jsonstate bad";
    }
  }
}
function initEditors() {
  document.querySelectorAll("textarea[data-editor]").forEach(updateEditor);
}
document.addEventListener("DOMContentLoaded", initEditors);
document.addEventListener("htmx:afterSettle", initEditors);
document.addEventListener("input", (ev) => {
  if (ev.target.matches("textarea[data-editor]")) updateEditor(ev.target);
});
document.addEventListener("scroll", (ev) => {
  if (ev.target.matches && ev.target.matches("textarea[data-editor]")) {
    const g = ev.target.parentElement.querySelector(".gutter");
    if (g) g.scrollTop = ev.target.scrollTop;
  }
}, true);
document.addEventListener("change", (ev) => {
  const t = ev.target;
  if (t.matches("[data-select-all]")) {
    t.closest("table").querySelectorAll("tbody input[type=checkbox]").forEach((c) => (c.checked = t.checked));
  }
  if (t.matches("select[name=contentType]")) {
    const ta = t.closest("form").querySelector("textarea[data-editor]");
    if (ta) updateEditor(ta);
  }
  if (t.matches("[data-fill]") && t.files && t.files[0]) {
    t.files[0].text().then((text) => {
      const ta = t.closest("form").querySelector("textarea[name=" + t.dataset.fill + "]");
      ta.value = text;
      const type = t.closest("form").querySelector("[name=contentType]");
      if (type) type.value = "application/json";
      updateEditor(ta);
    });
  }
  if (t.matches("[data-repeat-toggle]")) {
    const box = t.closest("form").querySelector("[data-repeat]");
    box.hidden = !t.checked;
    if (!t.checked) box.querySelector("[name=repeat]").value = "1";
  }
});
document.addEventListener("click", (ev) => {
  const add = ev.target.closest("[data-add-row]");
  if (add) {
    const tpl = document.getElementById("prop-template");
    document.getElementById(add.dataset.addRow).appendChild(tpl.content.cloneNode(true));
  }
  if (ev.target.closest("[data-remove-row]")) {
    ev.target.closest(".proprow").remove();
  }
  if (ev.target.closest("[data-close-drawer]")) {
    document.getElementById("send").hidden = true;
  }
});

// Radio pills outside a refreshed fragment keep their highlight in sync.
document.addEventListener("change", (ev) => {
  const t = ev.target;
  if (t.matches(".pill input[type=radio]")) {
    t.closest(".pills").querySelectorAll(".pill").forEach((p) => p.classList.toggle("on", p.contains(t)));
  }
});
