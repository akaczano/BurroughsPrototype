//package com.viasat.burroughs.ConsoleLogger;
package com.viasat.burroughs.execution;

import com.viasat.burroughs.Logger;

public class DebugLevels {

	private static String debugLevel = "";
	private static String debugLevel2 = "";
	/*
	public DebugLevels() {
		debugLevel = "";
		debugLevel2 = "";
	}*/

	public static void clearDebugLevels() {
		DebugLevels.debugLevel = "";
		DebugLevels.debugLevel2 = "";
	}

	public static void appendDebugLevel(String input) {
		DebugLevels.debugLevel += input + '\n';
	}

	public static void appendDebugLevel2(String input) {
		DebugLevels.debugLevel2 += input + '\n';
	}

	public static void displayDebugLevel() {
		Logger.getLogger().writeLine(DebugLevels.debugLevel);
	}

	public static void displayDebugLevel2() {
		Logger.getLogger().writeLine(DebugLevels.debugLevel2);
	}



}



