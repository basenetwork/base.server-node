var countPassed;
var countFailed;
var countAssert;
var countTests;

module.exports = {
    start: function(tests) {
        countPassed = 0;
        countFailed = 0;
        countAssert = 0;
        countTests = 0;

        console.log(
            "-------------------------------------------------------------\n",
            "Testing started at " + new Date()
        );

        for(var name in tests) {
            if(name.substr(0, 4) != "Test") continue;
            var fn = tests[name];
            if(typeof fn !== "function") continue;
            countTests++;
            try {
                fn.call(tests);
            } catch(e) {
                countFailed++;
                console.log("\nFAIL. "+ name, "\nError:\n", e, "\n\n");
            }
        }
        console.log(
            "\n\n",
            (countFailed? "FAIL" : "OK")+ ".",
            countTests, "tests;",
            countAssert, "assert (",
            countPassed, "passed /",
            countFailed, "failed )",
            "\n"
        );
    },

    equal: function(expected, value) {
        countAssert++;
        process.stdout.write(".");
        expected = JSON.stringify(expected);
        value = JSON.stringify(value);
        if(expected !== value) {
            throw "\tExpected: "+expected + "\n\tActual:   "+value;
        }
        countPassed++;
        return this;
    }
};