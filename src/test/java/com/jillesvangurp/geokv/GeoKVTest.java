package com.jillesvangurp.geokv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.jillesvangurp.geo.GeoHashUtils;

@Test
public class GeoKVTest {
    ValueProcessor<String> stringProcessor = new ValueProcessor<String>() {

        @Override
        public String serialize(String v) {
            return v;
        }

        @Override
        public String parse(String blob) {
            return blob;
        }
    };
    private Path dataDir;

    @BeforeMethod
    public void before() throws IOException {
        dataDir = Files.createTempDirectory("geokvdatadir");
    }
    @AfterMethod
    public void afterEach() throws IOException {
        FileUtils.deleteDirectory(dataDir.toFile());
    }

    private GeoKV<String> kv() {
        return new GeoKV<>(dataDir.toFile().getAbsolutePath(), 5,5, stringProcessor);
    }

    public void shouldPutAndGet() throws IOException {
        try(GeoKV<String> kv = kv()) {
            for(int i=0; i<90 ; i++) {
                kv.put(i, i, ""+i, ""+i);
            }
            assertThat(kv.get("88"), is("88"));
            assertThat(kv.get("1"), is("1"));
        }
    }

    public void shouldBeAbleToReadValuesAfterReopen() throws IOException {
        try(GeoKV<String> kv = kv()) {
            for(int i=0; i<90 ; i++) {
                kv.put(i, i, ""+i, ""+i);
                kv.put(i, i, ""+i*i, ""+i*i);
            }
        }
        try(GeoKV<String> kv = kv()) {
            for(int i=0; i<90 ; i++) {
                assertThat(kv.get(""+i), is(""+i));
            }
        }
    }

    public void shouldBeAbleToIterateOverEverything() throws IOException {
        try(GeoKV<String> kv = kv()) {
            for(int i=0; i<90 ; i++) {
                kv.put(i, i, ""+i, ""+i);
            }
            int count=0;
            for(String v: kv) {
                assertThat(v, notNullValue());
                count++;
            }
            assertThat(count,is(90));
        }
    }

    @DataProvider
    public Object[][] illegalKeys() {
        return new String[][] {
                {"\t"},
                {"\n"},
                {"xxxx\t"},
                {"xxxx\n"},
                {null},
                {""}
                };
    }

    @Test(expectedExceptions=IllegalArgumentException.class, dataProvider="illegalKeys")
    public void shouldDisallowKeys(String key) throws IOException {
        try(GeoKV<String> kv = kv()) {
            kv.put(0, 0, key, key + " is not allowed!");
        }
    }

    public void shouldRemove() throws IOException {
        try(GeoKV<String> kv = kv()) {
            for(int i=0; i<90 ; i++) {
                kv.put(i, i, ""+i, ""+i);
            }
            int size = kv.size();
            assertThat(kv.remove("42"), is("42"));
            assertThat(kv.remove("42"), nullValue());
            assertThat(kv.size(),is(size-1));
        }
    }

    @Test(expectedExceptions=NullPointerException.class)
    public void shouldThrowNPEWhenRemovingNullKey() throws IOException {
        try(GeoKV<String> kv = kv()) {
            kv.remove(null);
        }
    }

    public void shouldIterateOverLargeGeoHash() throws IOException {
        try(GeoKV<String> kv = kv()) {
            fillKv(kv);
            int count=0;
            for(Entry<String,String> v: kv.filterGeoHash("u33")) {
                assertThat("only entries with the right hash prefix should be returned",v.getValue().startsWith("u"));
                count++;
            }
            assertThat("only return stuff near berlin (u33)",count, allOf(greaterThan(0), lessThan(101)));
        }
    }
    public void shouldIterateOverSmallGeoHash() throws IOException {
        try(GeoKV<String> kv = kv()) {
            fillKv(kv);
            for(String id: kv.keySet()) {
                String hash=kv.get(id);
                String prefix = hash.substring(0, 5);
                int count=0;
                for(Entry<String,String> h: kv.filterGeoHash(prefix)) {
                    assertThat("", h.getValue().startsWith(prefix));
                    count++;
                }
                assertThat(count, greaterThan(0));
            }
        }
    }

    public void shouldIterateOverManyGeoHashes() throws IOException {
        try(GeoKV<String> kv = kv()) {
            fillKv(kv);
            Iterator<String> hashes = kv.bucketGeoHashes().iterator();
            assertThat(countIterable(kv.filterGeoHashes(hashes.next(),hashes.next(),hashes.next())), greaterThan(2));
        }
    }

    public void shouldIterateWithCircle() throws IOException {
        try(GeoKV<String> kv = kv()) {
            fillKv(kv);
            assertThat(countIterable(kv.filterRadius(52, 13, 200000)), greaterThan(99));
        }
    }

    public static class CoordinateRandomizer {
        private static Random random = new Random();

        public static double[] next(double baseLatitude, double baseLongitude, int div) {
            double latitude = baseLatitude+random.nextDouble()/div;
            double longitude = baseLongitude+random.nextDouble()/div;
            return new double[] {latitude,longitude};
        }
    }

    private void fillKv(GeoKV<String> kv) {
        for(int i=0; i<100 ; i++) {
            putHash(kv, 52, 13,1);
            putHash(kv, 60, 24,1);
        }
    }
    private void putHash(GeoKV<String> kv, double baseLatitude, double baseLongitude,int div) {
        double[] point = CoordinateRandomizer.next(baseLatitude,baseLongitude,div);
        String hash = GeoHashUtils.encode(point[0], point[1]);
        kv.put(point[0], point[1], hash, hash);
    }
    private <T> int countIterable(Iterable<T> iterable) {
        int count=0;
        for(@SuppressWarnings("unused") T value: iterable) {
            count++;
        }
        return count;
    }

    public void shouldSupportMultipleValuesForSameCoordinates() throws IOException {
        try(GeoKV<String> kv = kv()) {
            for(int i=0; i< 10000; i++) {
                double latitude = 1 * i%5+1;
                kv.put(latitude, 1.42, ""+i, ""+i);
            }
            assertThat(countIterable(kv), is(10000));
        }
    }
}