/** Раскладка «карты трат»: прямоугольники с площадью по весу, поближе к квадратам. */

export interface Rect {
  x: number; // доли ширины и высоты поля: 0…1
  y: number;
  w: number;
  h: number;
}

/** Худшее отношение сторон в ряду площадей вдоль стороны side. */
function worst(row: number[], side: number): number {
  const sum = row.reduce((total, area) => total + area, 0);
  const square = side * side;
  return Math.max(
    (square * Math.max(...row)) / (sum * sum),
    (sum * sum) / (square * Math.min(...row)),
  );
}

/**
 * Прямоугольники для весов в их порядке (от больших к меньшим) на поле
 * width × height — «квадратная» раскладка: ряд набирается вдоль короткой стороны,
 * пока плитки не начнут вытягиваться. Веса не больше нуля — без плитки (нулевой
 * прямоугольник).
 */
export function treemap(
  weights: number[],
  width: number,
  height: number,
): Rect[] {
  const total = weights.reduce((sum, value) => sum + Math.max(value, 0), 0);
  const found: Rect[] = weights.map(() => ({ x: 0, y: 0, w: 0, h: 0 }));
  if (total <= 0 || width <= 0 || height <= 0) return found;
  const tiles = weights
    .map((value, index) => ({
      index,
      area: (Math.max(value, 0) / total) * width * height,
    }))
    .filter((tile) => tile.area > 0);
  let left = { x: 0, y: 0, w: width, h: height };
  let row: typeof tiles = [];

  const place = () => {
    const sum = row.reduce((sumOf, tile) => sumOf + tile.area, 0);
    if (left.w >= left.h) {
      // столбец слева на всю высоту остатка
      const wide = sum / left.h;
      let y = left.y;
      for (const tile of row) {
        const tall = tile.area / wide;
        found[tile.index] = { x: left.x, y, w: wide, h: tall };
        y += tall;
      }
      left = { x: left.x + wide, y: left.y, w: left.w - wide, h: left.h };
    } else {
      // ряд сверху на всю ширину остатка
      const tall = sum / left.w;
      let x = left.x;
      for (const tile of row) {
        const wide = tile.area / tall;
        found[tile.index] = { x, y: left.y, w: wide, h: tall };
        x += wide;
      }
      left = { x: left.x, y: left.y + tall, w: left.w, h: left.h - tall };
    }
    row = [];
  };

  for (const tile of tiles) {
    const side = Math.min(left.w, left.h);
    const areas = row.map((item) => item.area);
    if (
      row.length > 0 &&
      worst([...areas, tile.area], side) > worst(areas, side)
    ) {
      place();
    }
    row.push(tile);
  }
  if (row.length > 0) place();
  return found.map((rect) => ({
    x: rect.x / width,
    y: rect.y / height,
    w: rect.w / width,
    h: rect.h / height,
  }));
}
