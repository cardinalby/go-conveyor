// 15-color palette assigned to an item by its number modulo the palette size — see colorForItem.
const PALETTE = [
  "#4dddff",
  "#ff951c",
  "#3a25cc",
  "#aeed28",
  "#64197e",
  "#ffd00b",
  "#d488ff",
  "#46f6a6",
  "#ff6aa6",
  "#326c00",
  "#463372",
  "#01c8a7",
  "#983100",
  "#c1caff",
  "#ffbbd1",
];

export function colorForItem(itemNo: number): string {
  const i = ((itemNo % PALETTE.length) + PALETTE.length) % PALETTE.length;
  return PALETTE[i];
}

// A handful of these swatches are pale (yellow, lime, mint, periwinkle, pink), where white text would be nearly
// invisible — so the number's own color is picked per-item for contrast rather than fixed at white.
function relativeLuminance(hex: string): number {
  const [r, g, b] = [0, 2, 4].map((i) => parseInt(hex.slice(i + 1, i + 3), 16) / 255);
  const lin = (c: number) => (c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4);
  return 0.2126 * lin(r) + 0.7152 * lin(g) + 0.0722 * lin(b);
}

export function textColorForItem(itemNo: number): string {
  return relativeLuminance(colorForItem(itemNo)) > 0.55 ? "#111111" : "#ffffff";
}
