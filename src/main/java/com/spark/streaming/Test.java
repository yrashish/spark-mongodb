package com.spark.streaming;


	interface Gadget {
		void doStuff();
	}

	abstract class Electronic {
		void getPower() {
			System.out.print("plug in ");
		}
	}

	public class Test extends Electronic implements Gadget {
		 public void doStuff() {
			System.out.print("show book ");
		}

		public static void main(String[] args) {
			new Test().getPower();
			new Test().doStuff();
		}
	}


