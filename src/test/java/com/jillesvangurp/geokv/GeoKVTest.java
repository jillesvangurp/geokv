package com.jillesvangurp.geokv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
    
    public void before() throws IOException {
        dataDir = Files.createTempDirectory("geokvdatadir");        
    }
    @AfterMethod
    public void afterEach() throws IOException {
        FileUtils.deleteDirectory(dataDir.toFile());
    }

    public void shouldPutAndGet() throws IOException {        
        try(GeoKV<String> kv = new GeoKV<>(dataDir.toFile().getAbsolutePath() , 5, stringProcessor )) {
            for(int i=0; i<90 ; i++) {
                kv.put(i, i, ""+i, ""+i);
            }
            assertThat(kv.get("88"), is("88"));
            assertThat(kv.get("1"), is("1"));
        }
    }
    
    public void shouldBeAbleToReadValuesAfterReopen() throws IOException {
        try(GeoKV<String> kv = new GeoKV<>(dataDir.toFile().getAbsolutePath() , 5, stringProcessor )) {
            for(int i=0; i<90 ; i++) {
                kv.put(i, i, ""+i, ""+i);                
                kv.put(i, i, ""+i*i, ""+i*i);
            }
        }
        try(GeoKV<String> kv = new GeoKV<>(dataDir.toFile().getAbsolutePath() , 5, stringProcessor )) {
            for(int i=0; i<90 ; i++) {
                assertThat(kv.get(""+i), is(""+i));
            }
        }
    }
    
    public void shouldBeAbleToIterateOverEverything() throws IOException {
        try(GeoKV<String> kv = new GeoKV<>(dataDir.toFile().getAbsolutePath() , 5, stringProcessor )) {
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
        try(GeoKV<String> kv = new GeoKV<>(dataDir.toFile().getAbsolutePath(), 5, stringProcessor )) {
            kv.put(0, 0, key, key + " is not allowed!");
        }
    }
    
    public void shouldRemove() throws IOException {
        try(GeoKV<String> kv = new GeoKV<>(dataDir.toFile().getAbsolutePath() , 5, stringProcessor )) {
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
        try(GeoKV<String> kv = new GeoKV<>(dataDir.toFile().getAbsolutePath() , 5, stringProcessor )) {
            kv.remove(null);
        }        
    }
}