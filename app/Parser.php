<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    private const int BUCKETS = 256;
    private const int WRITE_BUFFER_SIZE = 1024 * 1024;
    private const string TMP_PREFIX = 'challenge_';
    private const int HOST_PREFIX_LEN = 19; // strlen('https://stitcher.io')

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

        while (($line = fgets($in)) !== false) {
            $line = rtrim($line, "\r\n");

            $commaPos = strpos($line, ',');
            $date = substr($line, $commaPos + 1, 10);

            $path = substr($line, self::HOST_PREFIX_LEN, $commaPos - self::HOST_PREFIX_LEN);

            if (!isset($seenPaths[$path])) {
                $seenPaths[$path] = true;
                $pathOrder[] = $path;
            }

            $bucket = crc32($path) & (self::BUCKETS - 1);
            fwrite($bucketHandles[$bucket], $path . "\t" . $date . "\n");
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

            while (($row = fgets($bh)) !== false) {
                $row = rtrim($row, "\r\n");

                $tabPos = strpos($row, "\t");
                $path = substr($row, 0, $tabPos);
                $date = substr($row, $tabPos + 1);

                $allCounts[$path][$date] = ($allCounts[$path][$date] ?? 0) + 1;
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

            $buf = $firstPath ? '' : ',';
            $firstPath = false;

            $buf .= "\n    \"{$escapedPath}\": {\n";

            $firstDate = true;
            foreach ($allCounts[$path] as $date => $count) {
                if (!$firstDate) {
                    $buf .= ",\n";
                }
                $firstDate = false;
                $buf .= "        \"{$date}\": {$count}";
            }

            $buf .= "\n    }";

            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}