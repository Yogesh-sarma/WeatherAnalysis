package ClimateChange;

import java.util.Scanner;

public class PrimeRank {
    public static void main(String[] args){
        while(true){
            System.out.println("Enter a name(or \"exit\") - ");
            Scanner sc = new Scanner(System.in);
            String name = sc.nextLine().trim();
            if(name.equalsIgnoreCase("exit")){
                System.exit(0);
            }
            int primeRank = name.toLowerCase().chars().filter(c -> isPrime(c-96)).map(c-> c-96).sum();
            if(primeRank == 0){
                System.out.println("This guy isn't a PRIME DUDE!!!!!");
            }
            else {
                System.out.println("Prime rank for "+name+" is "+primeRank);
            }
        }
    }

    private static boolean isPrime(int c){
        if(c==2){
            return true;
        }
        if(c>2){
            for(int n=2; n<Math.sqrt(c)+1;n++){
                if(c%n==0){
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
