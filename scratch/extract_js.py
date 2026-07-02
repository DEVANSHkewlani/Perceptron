import re

with open("dashboard/static/index.html", "r") as f:
    html = f.read()

# Find all script tags using a regex
scripts = re.findall(r"<script>(.*?)</script>", html, re.DOTALL)

with open("scratch/extracted.js", "w") as f:
    for idx, script in enumerate(scripts):
        f.write(f"// --- Script Block {idx} ---\n")
        f.write(script)
        f.write("\n")

print("JavaScript extracted successfully to scratch/extracted.js")
