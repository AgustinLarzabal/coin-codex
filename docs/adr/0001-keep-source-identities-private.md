# Keep Source Identities Private

CoinCodex will keep external source names, domains, and start URLs in private configuration or database records rather than hard-coding them into application logic, filenames, queue names, logs, or public UI. This adds configuration indirection, but protects operationally sensitive source choices and lets the codebase speak in generic terms such as Source and Source Adapter.
