module.exports = {
    proxy: "localhost:8000",
    files: ["**/*.html", "**/*.css", "**/*.js"],
    ignore: ["node_modules"],
    reloadDelay: 10,
    online: false,
    open: false,
    enableUI: false,
    injectChanges: true,
};
