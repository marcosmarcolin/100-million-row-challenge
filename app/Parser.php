<?php

namespace App;

final class Parser
{
    private const int CHUNK_SIZE = 8 * 1024 * 1024;
    private const int PREFIX_LEN = 25;
    private const int YEAR_START = 2020;
    private const int YEAR_END = 2026;

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $totalDates = (self::YEAR_END - self::YEAR_START + 1) * 12 * 31; // 7 * 372 = 2604
        $dateStrings = $this->buildDateStrings();

        $pathIds = [];
        $paths = [];
        $counts = [];

        $h = \fopen($inputPath, 'rb');

        $carry = '';
        while (!\feof($h)) {
            $chunk = \fread($h, self::CHUNK_SIZE);
            if ($chunk === false || $chunk === '') {
                break;
            }

            if ($carry !== '') {
                $chunk = $carry . $chunk;
            }

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) {
                $carry = $chunk;
                continue;
            }

            $carry = \substr($chunk, $lastNl + 1);
            $data = \substr($chunk, 0, $lastNl + 1);

            $offset = 0;
            while (($nl = \strpos($data, "\n", $offset)) !== false) {
                $lineLen = $nl - $offset;
                if ($lineLen <= 0) {
                    $offset = $nl + 1;
                    continue;
                }

                $line = \substr($data, $offset, $lineLen);
                $offset = $nl + 1;

                $comma = \strpos($line, ',');
                if ($comma === false || $comma <= self::PREFIX_LEN) {
                    continue;
                }

                $slug = \substr($line, self::PREFIX_LEN, $comma - self::PREFIX_LEN);

                $datePos = $comma + 1;
                if (!isset($line[$datePos + 9])) {
                    continue;
                }

                $dateId = $this->dateIdFromLine($line, $datePos);
                if ($dateId < 0 || $dateId >= $totalDates) {
                    continue;
                }

                if (!isset($pathIds[$slug])) {
                    $pathIds[$slug] = \count($paths);
                    $paths[] = $slug;

                    for ($i = 0; $i < $totalDates; $i++) {
                        $counts[] = 0;
                    }
                }

                $pIdx = $pathIds[$slug];
                $counts[($pIdx * $totalDates) + $dateId]++;
            }
        }

        \fclose($h);

        $this->writeFinalJson($outputPath, $paths, $dateStrings, $counts, $totalDates);
    }

    private function buildDateStrings(): array
    {
        $dateStrings = [];
        $idx = 0;

        for ($y = self::YEAR_START; $y <= self::YEAR_END; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $mS = $m < 10 ? '0' . $m : (string)$m;

                for ($d = 1; $d <= 31; $d++) {
                    $dS = $d < 10 ? '0' . $d : (string)$d;
                    $dateStrings[$idx++] = $y . '-' . $mS . '-' . $dS;
                }
            }
        }

        return $dateStrings;
    }

    private function dateIdFromLine(string $line, int $pos): int
    {
        // YYYY-MM-DD em $line[$pos..$pos+9]
        // id = (year-2020)*372 + (month-1)*31 + (day-1)
        $y =
            ((\ord($line[$pos]) - 48) * 1000) +
            ((\ord($line[$pos + 1]) - 48) * 100) +
            ((\ord($line[$pos + 2]) - 48) * 10) +
            (\ord($line[$pos + 3]) - 48);

        $m =
            ((\ord($line[$pos + 5]) - 48) * 10) +
            (\ord($line[$pos + 6]) - 48);

        $d =
            ((\ord($line[$pos + 8]) - 48) * 10) +
            (\ord($line[$pos + 9]) - 48);

        return ($y - self::YEAR_START) * 372 + ($m - 1) * 31 + ($d - 1);
    }

    private function writeFinalJson(string $outPath, array $paths, array $dateStrings, array $counts, int $totalDates): void
    {
        $fp = \fopen($outPath, 'wb');

        \fwrite($fp, "{\n");
        $firstSlug = true;

        foreach ($paths as $pIdx => $slug) {
            $base = $pIdx * $totalDates;

            $slugData = '';
            $hasData = false;
            $firstDate = true;

            for ($d = 0; $d < $totalDates; $d++) {
                $val = $counts[$base + $d] ?? 0;
                if ($val > 0) {
                    if (!$firstDate) {
                        $slugData .= ",\n";
                    }
                    $slugData .= "        \"{$dateStrings[$d]}\": {$val}";
                    $hasData = true;
                    $firstDate = false;
                }
            }

            if ($hasData) {
                if (!$firstSlug) {
                    \fwrite($fp, ",\n");
                }

                $escapedSlug = \str_replace('/', '\/', $slug);
                \fwrite($fp, "    \"\/blog\/{$escapedSlug}\": {\n{$slugData}\n    }");

                $firstSlug = false;
            }
        }

        \fwrite($fp, "\n}");
        \fclose($fp);
    }
}