package com.jillesvangurp.geokv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.jillesvangurp.geo.GeoGeometry;
import com.jillesvangurp.geo.GeoHashUtils;
import com.jillesvangurp.iterables.Filter;
import com.jillesvangurp.iterables.FilteringIterable;

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
    private final int bucketSize;
    private final String dataDir;
    private final HTreeMap<String, String> key2geohash;
    private final LoadingCache<String, Bucket> cache;
    private final ValueProcessor<Value> processor;
    private final Set<String> geoHashes;
    private final BucketLock bucketLock = new BucketLock();
    private final DB db;

    /**
     * Create a new GeoKV.
     *
     * You will need to take a few important decisions regarding bucket size and the amount of buckets you can cache.
     *
     * More and bigger is better since you will use the disk much less and in more efficient big bursts. This comes at
     * the price of more memory. Also, you should take into account the data density in your data set. Typical
     * metropoles in e.g. China or South America tend to have a high density of data for e.g. POI data. In that case, a
     * geoHash of 4 would not be appropriate since that would cover an area that might potentially contain millions of
     * data points. Something in the order of 6 or 7 would bring this number down to a maximum of maybe a few thousand,
     * which is manageable as a worst case scenario.
     *
     * Likewise, if you are planning to run lots of queries covering large areas, you will benefit from dedicating
     * heap-space to caching more buckets. If on the other hand your processing is highly localized, you don't benefit
     * from having more than a handful of buckets.
     *
     * If you are uncertain, go with sensible defaults of e.g. a bucketSize of 6 and having 200 buckets in memory.
     *
     * You may find the table below with dimensions for geohashes at different latitudes useful to decide on a good hash
     * code size. As you can see the horizontal width depends on the latitude. The height does not vary.
     * <table border="1">
     * <tr>
     * <td>latitude</td>
     * <td>1</td>
     * <td>2</td>
     * <td>3</td>
     * <td>4</td>
     * <td>5</td>
     * <td>6</td>
     * <td>7</td>
     * <td>8</td>
     * <td>9</td>
     * <td>10</td>
     * <td>11</td>
     * <td>12</td>
     * </tr>
     * <tr>
     * <td>90.0</td>
     * <td>3491488.98x5003771.7</td>
     * <td>122418.86x625471.46</td>
     * <td>3837.36x156367.87</td>
     * <td>119.93x19545.98</td>
     * <td>3.75x4886.5</td>
     * <td>0.12x610.81</td>
     * <td>0.0x152.7</td>
     * <td>0.0x19.09</td>
     * <td>0.0x4.77</td>
     * <td>0.0x0.6</td>
     * <td>0.0x0.15</td>
     * <td>0.0x0.02</td>
     * </tr>
     * <tr>
     * <td>80.0</td>
     * <td>3491488.98x5003771.7</td>
     * <td>243669.87x625471.46</td>
     * <td>30505.12x156367.87</td>
     * <td>6801.36x19545.98</td>
     * <td>850.17x4886.5</td>
     * <td>212.2x610.81</td>
     * <td>26.52x152.7</td>
     * <td>6.63x19.09</td>
     * <td>0.83x4.77</td>
     * <td>0.21x0.6</td>
     * <td>0.03x0.15</td>
     * <td>0.01x0.02</td>
     * </tr>
     * <tr>
     * <td>70.0</td>
     * <td>3491488.98x5003771.7</td>
     * <td>478058.65x625471.46</td>
     * <td>56274.79x156367.87</td>
     * <td>13395.26x19545.98</td>
     * <td>1674.41x4886.5</td>
     * <td>417.83x610.81</td>
     * <td>52.23x152.7</td>
     * <td>13.06x19.09</td>
     * <td>1.63x4.77</td>
     * <td>0.41x0.6</td>
     * <td>0.05x0.15</td>
     * <td>0.01x0.02</td>
     * </tr>
     * <tr>
     * <td>60.0</td>
     * <td>3491488.98x5003771.7</td>
     * <td>694214.17x625471.46</td>
     * <td>80387.66x156367.87</td>
     * <td>19580.57x19545.98</td>
     * <td>2444.33x4886.5</td>
     * <td>610.88x610.81</td>
     * <td>76.35x152.7</td>
     * <td>19.09x19.09</td>
     * <td>2.39x4.77</td>
     * <td>0.6x0.6</td>
     * <td>0.07x0.15</td>
     * <td>0.02x0.02</td>
     * </tr>
     * <tr>
     * <td>50.0</td>
     * <td>3491488.98x5003771.7</td>
     * <td>883838.56x625471.46</td>
     * <td>102133.77x156367.87</td>
     * <td>25168.62x19545.98</td>
     * <td>3143.21x4886.5</td>
     * <td>785.26x610.81</td>
     * <td>98.16x152.7</td>
     * <td>24.54x19.09</td>
     * <td>3.07x4.77</td>
     * <td>0.77x0.6</td>
     * <td>0.1x0.15</td>
     * <td>0.02x0.02</td>
     * </tr>
     * <tr>
     * <td>40.0</td>
     * <td>5003771.7x5003771.7</td>
     * <td>966365.48x625471.46</td>
     * <td>120872.77x156367.87</td>
     * <td>29988.95x19545.98</td>
     * <td>3743.81x4886.5</td>
     * <td>935.88x610.81</td>
     * <td>116.98x152.7</td>
     * <td>29.24x19.09</td>
     * <td>3.66x4.77</td>
     * <td>0.91x0.6</td>
     * <td>0.11x0.15</td>
     * <td>0.03x0.02</td>
     * </tr>
     * <tr>
     * <td>30.0</td>
     * <td>5003771.7x5003771.7</td>
     * <td>1102838.21x625471.46</td>
     * <td>136052.82x156367.87</td>
     * <td>33894.53x19545.98</td>
     * <td>4233.08x4886.5</td>
     * <td>1057.98x610.81</td>
     * <td>132.25x152.7</td>
     * <td>33.06x19.09</td>
     * <td>4.13x4.77</td>
     * <td>1.03x0.6</td>
     * <td>0.13x0.15</td>
     * <td>0.03x0.02</td>
     * </tr>
     * <tr>
     * <td>20.0</td>
     * <td>5003771.7x5003771.7</td>
     * <td>1196915.14x625471.46</td>
     * <td>147226.82x156367.87</td>
     * <td>36766.23x19545.98</td>
     * <td>4591.95x4886.5</td>
     * <td>1147.99x610.81</td>
     * <td>143.49x152.7</td>
     * <td>35.87x19.09</td>
     * <td>4.48x4.77</td>
     * <td>1.12x0.6</td>
     * <td>0.14x0.15</td>
     * <td>0.04x0.02</td>
     * </tr>
     * <tr>
     * <td>10.0</td>
     * <td>5003771.7x5003771.7</td>
     * <td>1244900.01x625471.46</td>
     * <td>154065.65x156367.87</td>
     * <td>38516.44x19545.98</td>
     * <td>4812.62x4886.5</td>
     * <td>1203.07x610.81</td>
     * <td>150.38x152.7</td>
     * <td>37.6x19.09</td>
     * <td>4.7x4.77</td>
     * <td>1.17x0.6</td>
     * <td>0.15x0.15</td>
     * <td>0.04x0.02</td>
     * </tr>
     * </table>
     * *
     *
     * @param dataDir
     *            GeoKV will store all its data here and be able to read an existing store if you have one.
     * @param cacheSize
     *            the number of buckets that GeoKV will keep in memory.
     * @param bucketSize
     *            the geoHash length for a bucket. A reasonable size will typically be between 4 an 8. Bigger buckets
     *            with a shorter geoHash contain more data.
     * @param processor
     *            processor that can serialize and parse data for storage.
     */
    public GeoKV(String dataDir, int cacheSize, int bucketSize, ValueProcessor<Value> processor) {
        this.dataDir = dataDir;
        this.bucketSize = bucketSize;
        this.processor = processor;

        db = DBMaker.newFileDB(getIdsPath()).cacheLRUEnable().cacheSize(100000).closeOnJvmShutdown().compressionEnable().make();
        this.key2geohash = db.getHashMap("key2geohash");
        this.geoHashes = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        CacheLoader<String, Bucket> loader = new CacheLoader<String, Bucket>() {
            @Override
            public Bucket load(String geohash) throws Exception {
                try {
                    bucketLock.acquire(geohash);
                    Bucket bucket = new Bucket(geohash);
                    bucket.read();
                    return bucket;
                } finally {
                   bucketLock.release(geohash);
                }
            }
        };
        cache = CacheBuilder.newBuilder().maximumSize(cacheSize).removalListener(new RemovalListener<String, Bucket>() {
            @Override
            public void onRemoval(RemovalNotification<String, Bucket> notification) {
                String geoHash = notification.getKey();
                try {
                    bucketLock.acquire(geoHash);
                    // make sure changed buckets are written on eviction
                    notification.getValue().write();
                } finally {
                    bucketLock.release(geoHash);
                }
            }
        }).build(loader);
//        readIds();
    }

    class BucketLock {
        private final Set<String> buckets = new ConcurrentSkipListSet<>();
        Lock bucketLock = new ReentrantLock();

        public void acquire(String bucket) {
            while(true) {
                try {
                    bucketLock.lock();
                    if(!buckets.contains(bucket)) {
                        buckets.add(bucket);
                        return;
                    }
                } finally {
                    bucketLock.unlock();
                }
            }
        }

        public void release(String bucket) {
            try {
                bucketLock.lock();
                buckets.remove(bucket);
            } finally {
                bucketLock.unlock();
            }
        }
    }

//    private void readIds() {
//        File idsFile = getIdsPath();
//        if (idsFile.exists()) {
//            try {
//                try (BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(idsFile)), Charset.forName("utf-8")))) {
//                    String line;
//                    while ((line = r.readLine()) != null) {
//                        if (StringUtils.isNotEmpty(line)) {
//                            int tab = line.indexOf('\t');
//                            if (tab < 0) {
//                                throw new IllegalStateException("line without a tab");
//                            } else {
//                                String key = line.substring(0, tab);
//                                String geoHash = line.substring(tab + 1);
//                                key2geohash.put(key, geoHash);
//                                geoHashes.add(geoHash);
//                            }
//                        }
//                    }
//                }
//            } catch (IOException e) {
//                throw new IllegalStateException(e);
//            }
//        }
//    }

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
            String hashPrefix = hash.substring(0, bucketSize);
            Bucket bucket = cache.get(hashPrefix);
            try {
                bucketLock.acquire(hashPrefix);
                key2geohash.put(key, hashPrefix);
//                necessary for working correctly but extremely slow
//                db.commit();
                geoHashes.add(hashPrefix);
                bucket.put(key, hash, value);
            } finally {
                bucketLock.release(hashPrefix);
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

    public Iterable<Entry<String, Value>> filterBbox(double minLat, double maxLat, double minLon, double maxLon) {
        double[][] polygon = GeoGeometry.bbox2polygon(new double[] { minLat, maxLat, minLon, maxLon });
        return filterPolygon(polygon);
    }

    public Iterable<Entry<String, Value>> filterPolygon(double[][] polygon) {
        Set<String> hashes = GeoHashUtils.geoHashesForPolygon(polygon);
        return filterGeoHashes(hashes.toArray(new String[0]));
    }

//    public Iterable<Entry<String, Value>> filterRadius(double latitude, double longitude, double meters) {
//        Set<String> hashes = GeoHashUtils.geoHashesForCircle(GeoHashUtils.getSuitableHashLength(meters, latitude, longitude), latitude, longitude, meters);
//        return filterGeoHashes(hashes.toArray(new String[0]));
//    }

    public Iterable<Entry<String, Value>> filterRadius(final double latitude, final double longitude, final double meters) {
        GeoGeometry.validate(latitude,longitude);
        double[][] polygon = GeoGeometry.bbox2polygon(GeoGeometry.bbox(latitude, longitude, meters, meters));
        Set<String> hashes = GeoHashUtils.geoHashesForPolygon(GeoHashUtils.suitableHashLength(meters, latitude, longitude),polygon);
        return new FilteringIterable<Entry<String, Value>>(filterGeoHashes(hashes.toArray(new String[0])), new Filter<Entry<String, Value>>() {
            @Override
            public boolean passes(Entry<String, Value> entry) {
                double[] decoded = GeoHashUtils.decode(entry.getKey());
                return GeoGeometry.distance(new double[] { longitude, latitude }, decoded) <= meters;
            }
        });
    }

    public Iterable<Entry<String, Value>> filterGeoHashes(final String... hashes) {
        final String[] sortedHashes = Arrays.copyOf(hashes, hashes.length);
        Arrays.sort(sortedHashes);

        return toIterable(new Iterator<Entry<String, Value>>() {
            int i = 0;
            Iterator<Entry<String, Value>> currentIt = null;
            Entry<String, Value> next = null;

            @Override
            public boolean hasNext() {
                if (next != null) {
                    return true;
                }
                if (currentIt == null || !currentIt.hasNext()) {
                    if (i < sortedHashes.length) {
                        currentIt = filterGeoHash(sortedHashes[i++]).iterator();
                        while (!currentIt.hasNext() && i < sortedHashes.length) {
                            currentIt = filterGeoHash(sortedHashes[i++]).iterator();
                        }
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
            public Entry<String, Value> next() {
                if (hasNext()) {
                    Entry<String, Value> result = next;
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

    public Iterable<Entry<String, Value>> filterGeoHash(final String hash) {
        try {
            if (hash.length() >= bucketSize) {
                // only one bucket
                Bucket bucket = cache.get(hash.substring(0, bucketSize));
                return toIterable(bucket.filter(hash));

            } else {
                final Iterator<String> geoHashesIterator = geoHashes.iterator();
                // multiple buckets
                return toIterable(new Iterator<Entry<String, Value>>() {
                    Iterator<Entry<String, Value>> currentIt = null;

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
                                    if (currentIt.hasNext()) {
                                        break;
                                    }
                                } catch (ExecutionException e) {
                                    throw new IllegalStateException(e);
                                }
                            }
                        }
                    }

                    @Override
                    public Entry<String, Value> next() {
                        if (hasNext()) {
                            Entry<String, Value> next = currentIt.next();
                            return next;
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
                try {
                    Bucket bucket = cache.get(hash);
                    bucketLock.acquire(hash);
                    key2geohash.remove(key);
                    return bucket.remove(key);
                } finally {
                    bucketLock.release(hash);
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
        db.close();
//        writeIds();
    }

//    private void writeIds() throws IOException, FileNotFoundException {
//        File f = getIdsPath();
//        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(f)), Charset.forName("utf-8")))) {
//            for (Entry<String, String> entry : key2geohash.entrySet()) {
//                bw.write(entry.getKey() + "\t" + entry.getValue() + "\n");
//            }
//        }
//    }

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

    private class Bucket implements Iterable<Entry<String, Value>> {
        Map<String, Object> map = new ConcurrentHashMap<>();
        Map<String, Set<Value>> geohashMap = new ConcurrentHashMap<>();
//        Multimap<String, Value> geohashMap = HashMultimap.create();
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
            Set<Value> set = geohashMap.get(hash);
            if(set == null) {
                set = Collections.newSetFromMap(new ConcurrentHashMap<Value,Boolean>());
                geohashMap.put(hash, set);
            }
            set.add(value);
            changed.set(true);
        }

        public Value get(String key) {
            Object row = map.get(key);
            if(row != null) {
                return extractValue(row);
            } else {
                return null;
            }
        }

        private String extractGeoHash(Object object) {
            return (String) ((Object[]) object)[0];
        }

        @SuppressWarnings("unchecked")
        private Value extractValue(Object object) {
            return (Value) ((Object[]) object)[1];
        }

        public Iterator<Entry<String, Value>> filter(final String hashPrefix) {
            final Iterator<Entry<String, Value>> it = new MultiMapIteratable<String, Value>(geohashMap).iterator();
            return new Iterator<Entry<String, Value>>() {
                Entry<String, Value> next = null;

                @Override
                public boolean hasNext() {
                    if (next != null) {
                        return true;
                    }

                    while (it.hasNext()) {
                        Entry<String, Value> entry = it.next();
                        if (entry.getKey().startsWith(hashPrefix)) {
                            next = entry;
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public Entry<String, Value> next() {
                    if (hasNext()) {
                        Entry<String, Value> result = next;
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
                            Set<Value> set = geohashMap.get(hash);
                            if(set == null) {
                                set = Collections.newSetFromMap(new ConcurrentHashMap<Value,Boolean>());
                                geohashMap.put(hash, set);
                            }
                            set.add(value);
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
        public Iterator<Entry<String, Value>> iterator() {
            return new MultiMapIteratable<String, Value>(geohashMap).iterator();
        }
    }

    private class MultiMapIteratable<K,V> implements Iterable<Entry<K,V>> {
        private final class MapEntry implements Entry<K, V> {
            private final K key;
            private final V value;

            public MapEntry(K key, V value) {
                this.key = key;
                this.value = value;
            }

            @Override
            public K getKey() {
                return key;
            }

            @Override
            public V getValue() {
                return value;
            }

            @Override
            public V setValue(V value) {
                throw new UnsupportedOperationException("immutable");
            }
        }

        private final Map<K, Set<V>> map;

        public MultiMapIteratable(Map<K,Set<V>> map) {
            this.map = map;
        }

        @Override
        public Iterator<Entry<K,V>> iterator() {
            return new Iterator<Entry<K,V>>() {
                Iterator<Entry<K,Set<V>>> mit = map.entrySet().iterator();
                Entry<K,Set<V>> currentME = null;
                Iterator<V> ci = null;
                V next = null;

                @Override
                public boolean hasNext() {
                    if(next!=null) {
                        return true;
                    } else {
                        if((currentME == null || !ci.hasNext()) && mit.hasNext()) {
                            currentME = mit.next();
                            ci = currentME.getValue().iterator();
                        }
                        if(ci != null && ci.hasNext()) {
                            next = ci.next();
                            return true;
                        } else  {
                            return false;
                        }
                    }
                }

                @Override
                public Entry<K, V> next() {
                    if(hasNext()) {
                        V result = next;
                        next = null;
                        return new MapEntry(currentME.getKey(),result);
                    } else {
                        throw new NoSuchElementException();
                    }

                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Remove is not supported");
                }
            };
        }
    }
}
