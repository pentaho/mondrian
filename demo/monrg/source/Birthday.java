/*
// $Id$
// MonRG example code.
*/
import happy.BirthdayResource;

import java.util.Locale;

public class Birthday {
    static void wishHappyBirthday(String name, int age) {
        if (age < 0) {
            throw BirthdayResource.instance().newTooYoung(name);
        }
        System.out.println(BirthdayResource.instance().getHappyBirthday(name, new Integer(age)));
    }
    public static void main(String[] args) {
        wishHappyBirthday("Fred", 33);
		try {
			wishHappyBirthday("Wilma", -3);
		} catch (Throwable e) {
			System.out.println("Received " + e);
		}
		BirthdayResource.setThreadLocale(Locale.FRANCE);
		wishHappyBirthday("Pierre", 22);
    }
}
