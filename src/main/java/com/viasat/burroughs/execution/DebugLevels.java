package com.viasat.burroughs.execution;
package com.viasat.burroughs;


public class DebugLevels {
	public static String debugLevel = "";
	public static String debugLevel2 = "";

	public void clearDebugLevels() {
		debugLevel = "";
		debugLevel2 = "";
	}

	public void appendDebugLevel(String input) {
		debugLevel += input + '\n';
	}

	public void appendDebugLevel2(String input) {
		debugLevel2 += input + '\n';
	}

	public void displayDebugLevel() {
		Logger.getLogger().writeLine(debugLevel);
	}

	public void displayDebugLevel2() {
		Logger.getLogger().writeLine(debugLevel2);


}



