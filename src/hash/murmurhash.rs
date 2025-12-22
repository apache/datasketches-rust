#[cfg(test)]
mod tests {
    #[test]
    fn test_remainder() {
        // remainder > 8
        let key = "The quick brown fox jumps over the lazy dog";
        let (h1, h2) = mur3::murmurhash3_x64_128(key.as_bytes(), 0);
        assert_eq!(h1, 0xe34bbc7bbc071b6c);
        assert_eq!(h2, 0x7a433ca9c49a9347);

        // change one bit
        let key = "The quick brown fox jumps over the lazy eog";
        let (h1, h2) = mur3::murmurhash3_x64_128(key.as_bytes(), 0);
        assert_eq!(h1, 0x362108102c62d1c9);
        assert_eq!(h2, 0x3285cd100292b305);

        // test a remainder < 8
        let key = "The quick brown fox jumps over the lazy dogdogdog";
        let (h1, h2) = mur3::murmurhash3_x64_128(key.as_bytes(), 0);
        assert_eq!(h1, 0x9c8205300e612fc4);
        assert_eq!(h2, 0xcbc0af6136aa3df9);

        // test a remainder = 8
        let key = "The quick brown fox jumps over the lazy1";
        let (h1, h2) = mur3::murmurhash3_x64_128(key.as_bytes(), 0);
        assert_eq!(h1, 0xe3301a827e5cdfe3);
        assert_eq!(h2, 0xbdbf05f8da0f0392);

        // test a remainder = 0
        let key = "The quick brown fox jumps over t";
        let (h1, h2) = mur3::murmurhash3_x64_128(key.as_bytes(), 0);
        assert_eq!(h1, 0xdf6af91bb29bdacf);
        assert_eq!(h2, 0x91a341c58df1f3a6);

        // test a ones byte and a zeros byte
        let key = [
            0x54, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69, 0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f, 0x77,
            0x6e, 0x20, 0x66, 0x6f, 0x78, 0x20, 0x6a, 0x75, 0x6d, 0x70, 0x73, 0x20, 0x6f, 0x76,
            0x65, 0x72, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6c, 0x61, 0x7a, 0x79, 0x20, 0x64, 0x6f,
            0x67, 0xff, 0x64, 0x6f, 0x67, 0x00,
        ];
        let (h1, h2) = mur3::murmurhash3_x64_128(&key, 0);
        assert_eq!(h1, 0xe88abda785929c9e);
        assert_eq!(h2, 0x96b98587cacc83d6);
    }
}
