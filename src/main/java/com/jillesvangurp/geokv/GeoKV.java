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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
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
 * GeoKV is a persistent key value store for geospatial values that caches the entries in memory for recently accessed areas.
 * 
 * This allows one to implement algorithms that e.g. access entries in a particular area without constantly having to access the disk. 
 * @param <Value>
 */
public class GeoKV<Value> implements Closeable, Iterable<Value> {
    private static final int GEOHASH_LENGTH = 5;
    private final String dataDir;
    private final Map<String, String> id2geohash;
    private final LoadingCache<String, Bucket> cache;
    private final ValueProcessor<Value> processor;

    public GeoKV(String dataDir, int buckets, ValueProcessor<Value> processor) {
        this.dataDir = dataDir;
        this.processor = processor;
        this.id2geohash = new ConcurrentHashMap<String, String>();
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
        if(idsFile.exists()) {
            try {
                try (BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(idsFile)), Charset.forName("utf-8")))) {
                    String line;
                    while((line=r.readLine()) != null) {
                        if(StringUtils.isNotEmpty(line)) {
                            int tab = line.indexOf('\t');
                            if(tab < 0) {
                                throw new IllegalStateException("line without a tab");
                            } else {
                                String key=line.substring(0, tab);
                                String geoHash = line.substring(tab+1);
                                id2geohash.put(key, geoHash);
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * @param latitude a wgs84 latitude between -90 to 90
     * @param longitude a wgs84 longitude between -180 and 180
     * @param key the key. Keys are not allowed to be empty or to contain tabs or new lines.
     * @param value
     */
    public void put(double latitude, double longitude, String key, Value value) {
        Validate.notEmpty(key);
        if(StringUtils.contains(key, '\t') || StringUtils.contains(key, '\n')) {
            throw new IllegalArgumentException("key must not contain new lines or tabs");
        }
        String hash = GeoHashUtils.encode(latitude, longitude, GEOHASH_LENGTH);        

        try {
            Bucket bucket = cache.get(hash);
            id2geohash.put(key, hash);
            bucket.put(key, value);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    public Value get(String key) {
        try {
            String hash = id2geohash.get(key);
            if(hash == null) {
                return null;
            } else {
                return cache.get(hash).get(key);
            }
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }
    
    public Value remove(String key) {
        try {
            String hash = id2geohash.get(key);
            if(hash == null) {
                return null;
            } else {
                id2geohash.remove(key);
                return cache.get(hash).remove(key);
            }
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }
    
    public int size() {
        return id2geohash.size();
    }

    @Override
    public void close() throws IOException {
        // force all buckets to be written
        cache.invalidateAll();
        writeIds();
    }

    private void writeIds() throws IOException, FileNotFoundException {
        File f = getIdsPath();
        try(BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(f)), Charset.forName("utf-8")))) {
            for(Entry<String, String> entry:id2geohash.entrySet()) {
                bw.write(entry.getKey()+"\t" +entry.getValue()+"\n");
            }
        }
    }

    private File getIdsPath() {
        return new File(dataDir,"ids.gz");
    }

    @Override
    public Iterator<Value> iterator() {
        final Multimap<String, String> idsForHash = HashMultimap.create();
        for(Entry<String, String> entry: id2geohash.entrySet()) {
            idsForHash.put(entry.getValue(), entry.getKey());
        }
        final Iterator<String> hashIterator = idsForHash.keySet().iterator();
        
        return new Iterator<Value>() {
            Value next=null;
            String currentHash=null;
            Iterator<String> keyIterator = null;

            @Override
            public boolean hasNext() {
                if(next!=null) {
                    return true;
                } else {
                    if(currentHash == null && hashIterator.hasNext()) {
                        currentHash = hashIterator.next();
                        keyIterator = idsForHash.get(currentHash).iterator();
                    }
                    if(!keyIterator.hasNext() && hashIterator.hasNext()) {
                        keyIterator = idsForHash.get(hashIterator.next()).iterator();                    
                    }
                    if(keyIterator.hasNext()) {
                        next=get(keyIterator.next());
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            @Override
            public Value next() {
                Value result = next;
                next=null;
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported");
            }
        };
    }

    private class Bucket {
        Map<String, Value> map = new ConcurrentHashMap<>();
        AtomicBoolean changed = new AtomicBoolean();
        private final String geoHash;

        private Bucket(String geoHash) {
            this.geoHash = geoHash;
        }

        public Value remove(String key) {
            return map.remove(key);
        }

        public void put(String key, Value value) {
            map.put(key, value);
            changed.set(true);
        }

        public Value get(String key) {
            return map.get(key);
        }

        public void write() {
            if (changed.get()) {
                try {
                    File f = getPath(geoHash);
                    
                    File dir = f.getParentFile();
                    if(!dir.exists()) {
                        dir.mkdirs();
                    }
                    try(BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(f)), Charset.forName("utf-8")))) {
                        for(Entry<String, Value> entry:map.entrySet()) {
                            bw.write(entry.getKey() +"\t" + processor.serialize(entry.getValue()) + "\n");
                        }
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        public void read() {
            File f = getPath(geoHash);
            if(f.exists()) {
                try (BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(f)), Charset.forName("utf-8")))) {
                    String line;
                    while((line=r.readLine()) != null) {
                        if(StringUtils.isNotEmpty(line)) {
                            int tab = line.indexOf('\t');
                            if(tab < 0) {
                                throw new IllegalStateException("line without a tab");
                            } else {
                                String key=line.substring(0, tab);
                                String blob = line.substring(tab+1);
                                map.put(key, processor.parse(blob));
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        private File getPath(String hash) {
            StringBuilder buf = new StringBuilder();
            
            for(char c: hash.toCharArray()) {
                buf.append(c);
                buf.append(File.separatorChar);
            }
            buf.deleteCharAt(buf.length()-1);
            return new File(new File(dataDir, buf.toString()), geoHash+".gz");
        }
    }
}
