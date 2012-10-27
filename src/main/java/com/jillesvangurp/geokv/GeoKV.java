package com.jillesvangurp.geokv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.jillesvangurp.geo.GeoHashUtils;

/**
 * GeoKV is a persistent key value store for geospatial values that caches the entries in memory for recently accessed
 * areas.
 * 
 * This allows one to implement algorithms that e.g. access entries in a particular area without constantly having to
 * access the disk.
 * 
 * @param <Value>
 */
public class GeoKV<Value> implements Closeable, Iterable<Value> {
    private static final int GEOHASH_LENGTH = 5;
    private final String dataDir;
    private final Map<String, String> key2geohash;
    private final LoadingCache<String, Bucket> cache;
    private final ValueProcessor<Value> processor;
    private final Set<String> geoHashes;

    public GeoKV(String dataDir, int buckets, ValueProcessor<Value> processor) {
        this.dataDir = dataDir;
        this.processor = processor;
        this.key2geohash = new ConcurrentHashMap<String, String>();
        this.geoHashes = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        CacheLoader<String, Bucket> loader = new CacheLoader<String, Bucket>() {
            @Override
            public Bucket load(String geohash) throws Exception {
                Bucket bucket = new Bucket(geohash);
                bucket.read();
                return bucket;
            }
        };
        cache = CacheBuilder.newBuilder().maximumSize(buckets).removalListener(new RemovalListener<String, Bucket>() {
            @Override
            public void onRemoval(RemovalNotification<String, Bucket> notification) {
                // make sure changed buckets are written on eviction
                notification.getValue().write();
            }
        }).build(loader);
        readIds();
    }

    private void readIds() {
        File idsFile = getIdsPath();
        if (idsFile.exists()) {
            try {
                try (BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(idsFile)), Charset.forName("utf-8")))) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        if (StringUtils.isNotEmpty(line)) {
                            int tab = line.indexOf('\t');
                            if (tab < 0) {
                                throw new IllegalStateException("line without a tab");
                            } else {
                                String key = line.substring(0, tab);
                                String geoHash = line.substring(tab + 1);
                                key2geohash.put(key, geoHash);
                                geoHashes.add(geoHash);
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public Set<String> bucketGeoHashes() {
        return Collections.unmodifiableSet(geoHashes);
    }

    public Set<String> keySet() {
        return Collections.unmodifiableSet(key2geohash.keySet());
    }

    /**
     * @param latitude
     *            a wgs84 latitude between -90 to 90
     * @param longitude
     *            a wgs84 longitude between -180 and 180
     * @param key
     *            the key. Keys are not allowed to be empty or to contain tabs or new lines.
     * @param value
     */
    public void put(double latitude, double longitude, String key, Value value) {
        Validate.notEmpty(key);
        if (StringUtils.contains(key, '\t') || StringUtils.contains(key, '\n')) {
            throw new IllegalArgumentException("key must not contain new lines or tabs");
        }
        String hash = GeoHashUtils.encode(latitude, longitude);

        try {
            String hashPrefix = hash.substring(0, GEOHASH_LENGTH);
            Bucket bucket = cache.get(hashPrefix);
            synchronized (this) {
                key2geohash.put(key, hashPrefix);
                geoHashes.add(hashPrefix);
                bucket.put(key, hash, value);
            }
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    public Value get(String key) {
        try {
            String hash = key2geohash.get(key);
            if (hash == null) {
                return null;
            } else {
                return cache.get(hash).get(key);
            }
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private <T> Iterable<T> toIterable(final Iterator<T> it) {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return it;
            }
        };
    }

    public Iterable<Value> filterGeoHashes(final String... hashes) {
        String[] sortedHashes = Arrays.copyOf(hashes, hashes.length);
        Arrays.sort(sortedHashes);

        return toIterable(new Iterator<Value>() {
            int i = 0;
            Iterator<Value> currentIt = null;
            Value next = null;

            @Override
            public boolean hasNext() {
                if(next != null) {
                    return true;
                }
                if (currentIt == null || !currentIt.hasNext()) {
                    if(i<hashes.length) {
                        currentIt = filterGeoHash(hashes[i++]).iterator();
                    } else {
                        return false;
                    }
                }
                if (currentIt == null || !currentIt.hasNext()) {
                    return false;
                }
                next = currentIt.next();
                return true;
            }

            @Override
            public Value next() {
                if (hasNext()) {
                    Value result = next;
                    next = null;
                    return result;
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported");
            }
        });
    }

    public Iterable<Value> filterGeoHash(final String hash) {
        try {
            if (hash.length() >= GEOHASH_LENGTH) {
                // only one bucket
                Bucket bucket = cache.get(hash.substring(0, GEOHASH_LENGTH));
                return toIterable(bucket.filter(hash));
            } else {
                final Iterator<String> geoHashesIterator = geoHashes.iterator();
                // multiple buckets
                return toIterable(new Iterator<Value>() {
                    Iterator<Value> currentIt = null;

                    @Override
                    public boolean hasNext() {
                        if (currentIt == null || !currentIt.hasNext()) {
                            getNextIt();
                        }
                        if (currentIt == null || !currentIt.hasNext()) {
                            return false;
                        }
                        return true;
                    }

                    private void getNextIt() {
                        while (geoHashesIterator.hasNext()) {
                            String nextHash = geoHashesIterator.next();
                            if (nextHash.startsWith(hash)) {
                                try {
                                    currentIt = cache.get(nextHash).iterator();
                                    break;
                                } catch (ExecutionException e) {
                                    throw new IllegalStateException(e);
                                }
                            }
                        }

                    }

                    @Override
                    public Value next() {
                        if (hasNext()) {
                            return currentIt.next();
                        } else {
                            throw new NoSuchElementException();
                        }
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("remove is not supported");
                    }
                });
            }
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    public Value remove(String key) {
        try {
            String hash = key2geohash.get(key);
            if (hash == null) {
                return null;
            } else {
                synchronized (this) {
                    key2geohash.remove(key);
                    return cache.get(hash).remove(key);
                }
            }
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    public int size() {
        return key2geohash.size();
    }

    @Override
    public void close() throws IOException {
        // force all buckets to be written
        cache.invalidateAll();
        writeIds();
    }

    private void writeIds() throws IOException, FileNotFoundException {
        File f = getIdsPath();
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(f)), Charset.forName("utf-8")))) {
            for (Entry<String, String> entry : key2geohash.entrySet()) {
                bw.write(entry.getKey() + "\t" + entry.getValue() + "\n");
            }
        }
    }

    private File getIdsPath() {
        return new File(dataDir, "ids.gz");
    }

    @Override
    public Iterator<Value> iterator() {
        final Multimap<String, String> idsForHash = HashMultimap.create();
        for (Entry<String, String> entry : key2geohash.entrySet()) {
            idsForHash.put(entry.getValue(), entry.getKey());
        }
        final Iterator<String> hashIterator = idsForHash.keySet().iterator();

        return new Iterator<Value>() {
            Value next = null;
            String currentHash = null;
            Iterator<String> keyIterator = null;

            @Override
            public boolean hasNext() {
                if (next != null) {
                    return true;
                } else {
                    if (currentHash == null && hashIterator.hasNext()) {
                        currentHash = hashIterator.next();
                        keyIterator = idsForHash.get(currentHash).iterator();
                    }
                    if (!keyIterator.hasNext() && hashIterator.hasNext()) {
                        keyIterator = idsForHash.get(hashIterator.next()).iterator();
                    }
                    if (keyIterator.hasNext()) {
                        next = get(keyIterator.next());
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            @Override
            public Value next() {
                Value result = next;
                next = null;
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported");
            }
        };
    }

    private class Bucket implements Iterable<Value> {
        Map<String, Object> map = new ConcurrentHashMap<>();
        Map<String, Value> geohashMap = new ConcurrentHashMap<>();
        AtomicBoolean changed = new AtomicBoolean();
        private final String geoHash;

        private Bucket(String geoHash) {
            this.geoHash = geoHash;
        }

        public Value remove(String key) {
            return extractValue(map.remove(key));
        }

        public void put(String key, String hash, Value value) {
            map.put(key, new Object[] { hash, value });
            geohashMap.put(hash, value);
            changed.set(true);
        }

        public Value get(String key) {
            return extractValue(map.get(key));
        }

        private String extractGeoHash(Object object) {
            return (String) ((Object[]) object)[0];
        }

        @SuppressWarnings("unchecked")
        private Value extractValue(Object object) {
            return (Value) ((Object[]) object)[1];
        }

        public Iterator<Value> filter(final String hashPrefix) {
            final Iterator<Entry<String, Value>> it = geohashMap.entrySet().iterator();
            return new Iterator<Value>() {
                Value next = null;

                @Override
                public boolean hasNext() {
                    if (next != null) {
                        return true;
                    }

                    while (it.hasNext()) {
                        Entry<String, Value> entry = it.next();
                        if (entry.getKey().startsWith(hashPrefix)) {
                            next = entry.getValue();
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public Value next() {
                    if (hasNext()) {
                        Value result = next;
                        next = null;
                        return result;
                    } else {
                        throw new NoSuchElementException();
                    }
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("remove is not supported");
                }
            };
        }

        public void write() {
            if (changed.get()) {
                try {
                    File f = getPath(geoHash);

                    File dir = f.getParentFile();
                    if (!dir.exists()) {
                        dir.mkdirs();
                    }
                    try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(f)), Charset.forName("utf-8")))) {
                        for (Entry<String, Object> entry : map.entrySet()) {
                            Object object = entry.getValue();
                            bw.write(entry.getKey() + "\t" + extractGeoHash(object) + "\t" + processor.serialize(extractValue(object)) + "\n");
                        }
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        public void read() {
            File f = getPath(geoHash);
            if (f.exists()) {
                try (BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(f)), Charset.forName("utf-8")))) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        if (StringUtils.isNotEmpty(line)) {
                            int tab = line.indexOf('\t');
                            if (tab < 0) {
                                throw new IllegalStateException("line without a tab");
                            }
                            String key = line.substring(0, tab);
                            String rest = line.substring(tab + 1);
                            tab = rest.indexOf('\t');
                            if (tab < 0) {
                                throw new IllegalStateException("line without second tab");
                            }
                            String hash = rest.substring(0, tab);
                            String blob = rest.substring(tab + 1);
                            Value value = processor.parse(blob);
                            map.put(key, new Object[] { hash, value });
                            geohashMap.put(hash, value);
                        }
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        private File getPath(String hash) {
            StringBuilder buf = new StringBuilder();

            for (char c : hash.toCharArray()) {
                buf.append(c);
                buf.append(File.separatorChar);
            }
            buf.deleteCharAt(buf.length() - 1);
            return new File(new File(dataDir, buf.toString()), geoHash + ".gz");
        }

        @Override
        public Iterator<Value> iterator() {
            final Iterator<Value> it = geohashMap.values().iterator();
            return new Iterator<Value>() {

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Value next() {
                    return it.next();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("remove is not supported");
                }
            };
        }
    }
}
