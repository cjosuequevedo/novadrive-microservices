"""
checkpoint_customers_responsive.py - Verificacion real (Playwright, no
"deberia verse bien") de que /customers no produce scroll horizontal en
ninguno de los 3 anchos acordados (celular, laptop, monitor grande) -
estandar de interfaz del 9 sep 2026 en CLAUDE.md.

No es parte del runtime - script de evidencia desechable, mismo
espiritu que los checkpoint_*.py de Andes.
"""

from playwright.sync_api import sync_playwright

URL = "http://127.0.0.1:8001/customers"
VIEWPORTS = [
    ("phone", 375, 812),
    ("laptop", 1366, 768),
    ("wide monitor", 1920, 1080),
]


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(http_credentials={"username": "admin", "password": "changeme"})

        for name, width, height in VIEWPORTS:
            page.set_viewport_size({"width": width, "height": height})
            page.goto(URL, wait_until="networkidle")

            scroll_width = page.evaluate("document.documentElement.scrollWidth")
            client_width = page.evaluate("document.documentElement.clientWidth")
            has_horizontal_scroll = scroll_width > client_width

            screenshot_path = f"scripts/_responsive_{name.replace(' ', '_')}.png"
            page.screenshot(path=screenshot_path, full_page=True)

            status = "FAIL - horizontal scroll" if has_horizontal_scroll else "OK - no horizontal scroll"
            print(
                f"[{name:12s}] {width}x{height} -> scrollWidth={scroll_width} "
                f"clientWidth={client_width} :: {status} (screenshot: {screenshot_path})"
            )
            assert not has_horizontal_scroll, f"{name} ({width}px) triggers horizontal scroll on /customers"

        browser.close()

    print("\nCHECKPOINT RESPONSIVE /customers: TODO OK en los 3 anchos, evidencia real (screenshots + medicion DOM)")


if __name__ == "__main__":
    main()
