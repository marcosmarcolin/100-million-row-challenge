<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    private const int BUCKETS = 256;

    private const int READ_CHUNK_SIZE = 8 * 1024 * 1024;
    private const int WRITE_BUFFER_SIZE = 1024 * 1024;

    private const string TMP_PREFIX = 'challenge_';

    private const int HOST_PREFIX_LEN = 19; // strlen('https://stitcher.io')
    private const int DATE_LEN = 10;

    public function parse(string $inputPath, string $outputPath): void
    {
        $tmpDir = sys_get_temp_dir() . '/' . self::TMP_PREFIX . getmypid();
        if (!is_dir($tmpDir)) {
            mkdir($tmpDir, 0777, true);
        }

        $bucketPaths = [];
        $bucketHandles = [];

        for ($i = 0; $i < self::BUCKETS; $i++) {
            $bucketPaths[$i] = $tmpDir . '/b_' . $i . '.tmp';
            $h = fopen($bucketPaths[$i], 'wb');
            stream_set_write_buffer($h, self::WRITE_BUFFER_SIZE);
            $bucketHandles[$i] = $h;
        }

        $seenPaths = [];
        $pathOrder = [];

        $in = fopen($inputPath, 'rb');
        $carry = '';

        while (!feof($in)) {
            $chunk = fread($in, self::READ_CHUNK_SIZE);
            if ($chunk === '' || $chunk === false) {
                break;
            }

            $buf = $carry . $chunk;
            $lastNl = strrpos($buf, "\n");
            if ($lastNl === false) {
                $carry = $buf;
                continue;
            }

            $carry = substr($buf, $lastNl + 1);
            $buf = substr($buf, 0, $lastNl + 1);

            $pos = 0;

            while (true) {
                $nlPos = strpos($buf, "\n", $pos);
                if ($nlPos === false) {
                    break;
                }

                $lineEnd = $nlPos;
                if ($lineEnd > $pos && $buf[$lineEnd - 1] === "\r") {
                    $lineEnd--;
                }

                $commaPos = strpos($buf, ',', $pos);
                if ($commaPos === false || $commaPos >= $lineEnd) {
                    $pos = $nlPos + 1;
                    continue;
                }

                $date = substr($buf, $commaPos + 1, self::DATE_LEN);

                $pathStart = $pos + self::HOST_PREFIX_LEN;
                $path = substr($buf, $pathStart, $commaPos - $pathStart);

                if (!isset($seenPaths[$path])) {
                    $seenPaths[$path] = true;
                    $pathOrder[] = $path;
                }

                $bucket = crc32($path) & (self::BUCKETS - 1);
                fwrite($bucketHandles[$bucket], $path . "\t" . $date . "\n");

                $pos = $nlPos + 1;
            }
        }

        if ($carry !== '') {
            $line = rtrim($carry, "\r\n");
            if ($line !== '') {
                $commaPos = strpos($line, ',');
                $date = substr($line, $commaPos + 1, self::DATE_LEN);
                $path = substr($line, self::HOST_PREFIX_LEN, $commaPos - self::HOST_PREFIX_LEN);

                if (!isset($seenPaths[$path])) {
                    $seenPaths[$path] = true;
                    $pathOrder[] = $path;
                }

                $bucket = crc32($path) & (self::BUCKETS - 1);
                fwrite($bucketHandles[$bucket], $path . "\t" . $date . "\n");
            }
        }

        fclose($in);

        foreach ($bucketHandles as $h) {
            fclose($h);
        }

        $allCounts = [];

        for ($i = 0; $i < self::BUCKETS; $i++) {
            $bucketFile = $bucketPaths[$i];
            if (!is_file($bucketFile)) {
                continue;
            }

            $bh = fopen($bucketFile, 'rb');
            $carry = '';

            while (!feof($bh)) {
                $chunk = fread($bh, self::READ_CHUNK_SIZE);
                if ($chunk === '' || $chunk === false) {
                    break;
                }

                $buf = $carry . $chunk;
                $lastNl = strrpos($buf, "\n");
                if ($lastNl === false) {
                    $carry = $buf;
                    continue;
                }

                $carry = substr($buf, $lastNl + 1);
                $buf = substr($buf, 0, $lastNl + 1);

                $pos = 0;

                while (true) {
                    $nlPos = strpos($buf, "\n", $pos);
                    if ($nlPos === false) {
                        break;
                    }

                    $lineEnd = $nlPos;
                    if ($lineEnd > $pos && $buf[$lineEnd - 1] === "\r") {
                        $lineEnd--;
                    }

                    $tabPos = strpos($buf, "\t", $pos);
                    if ($tabPos === false || $tabPos >= $lineEnd) {
                        $pos = $nlPos + 1;
                        continue;
                    }

                    $path = substr($buf, $pos, $tabPos - $pos);
                    $date = substr($buf, $tabPos + 1, self::DATE_LEN);

                    $allCounts[$path][$date] = ($allCounts[$path][$date] ?? 0) + 1;

                    $pos = $nlPos + 1;
                }
            }

            if ($carry !== '') {
                $line = rtrim($carry, "\r\n");
                if ($line !== '') {
                    $tabPos = strpos($line, "\t");
                    $path = substr($line, 0, $tabPos);
                    $date = substr($line, $tabPos + 1, self::DATE_LEN);
                    $allCounts[$path][$date] = ($allCounts[$path][$date] ?? 0) + 1;
                }
            }

            fclose($bh);
            unlink($bucketFile);
        }

        @rmdir($tmpDir);

        foreach ($allCounts as &$byDate) {
            ksort($byDate);
        }
        unset($byDate);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, self::WRITE_BUFFER_SIZE);
        fwrite($out, "{");

        $firstPath = true;

        foreach ($pathOrder as $path) {
            if (!isset($allCounts[$path])) {
                continue;
            }

            $escapedPath = str_replace('/', '\\/', $path);

            $bufOut = $firstPath ? '' : ',';
            $firstPath = false;

            $bufOut .= "\n    \"{$escapedPath}\": {\n";

            $firstDate = true;
            foreach ($allCounts[$path] as $date => $count) {
                if (!$firstDate) {
                    $bufOut .= ",\n";
                }
                $firstDate = false;
                $bufOut .= "        \"{$date}\": {$count}";
            }

            $bufOut .= "\n    }";

            fwrite($out, $bufOut);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}