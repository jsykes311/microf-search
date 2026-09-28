/* Directory filtering never overrides the existing group-access display rules. */
(() => {
  const query = document.getElementById("mf-tool-query");
  const form = document.getElementById("mf-tool-search");
  const empty = document.getElementById("mf-tools-empty");
  if (!query || !form) return;
  function filterTools() {
    const value = query.value.trim().toLowerCase();
    let total = 0;
    document.querySelectorAll(".mf-tool-group").forEach((group) => {
      let count = 0;
      group.querySelectorAll(".admin-card").forEach((card) => {
        const permitted = card.style.display !== "none";
        const matches = card.textContent.toLowerCase().includes(value);
        card.hidden = !matches;
        if (permitted && matches) count++;
      });
      group.hidden = count === 0;
      total += count;
    });
    empty.hidden = total > 0;
  }
  query.addEventListener("input", filterTools);
  query.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      query.value = "";
      filterTools();
    }
  });
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    filterTools();
  });
  const directory = document.querySelector(".mf-directory");
  new MutationObserver(filterTools).observe(directory, {
    subtree: true,
    attributes: true,
    attributeFilter: ["style"],
  });
  filterTools();
})();
