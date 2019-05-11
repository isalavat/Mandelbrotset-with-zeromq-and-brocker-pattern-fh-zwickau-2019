package loadbalancingbroker.domain;
public class Complex {
		public final double real;
		public final double imaginary;

		public Complex() {
			this(0.0, 0.0);
		}

		public Complex(double r, double i) {
			real = r;
			imaginary = i;
		}

		public Complex(Complex z) {
			this(z.real, z.imaginary);
		}

		public double abs() {
			return Math.sqrt((real * real) + (imaginary * imaginary));
		}

		public Complex add(Complex z) {
			return new Complex(real + z.real, imaginary + z.imaginary);
		}

		public Complex power(double x) {
			// siehe http://xahlee.info/java-a-day/ex_complex.html
			final double modulus = abs();
			final double arg = Math.atan2(imaginary, real);
			final double log_re = Math.log(modulus);
			final double log_im = arg;
			final double x_log_re = x * log_re;
			final double x_log_im = x * log_im;
			final double modulus_ans = Math.exp(x_log_re);
			return new Complex(modulus_ans * Math.cos(x_log_im), modulus_ans * Math.sin(x_log_im));
		}

		public Complex squared() {
			return power(2);
		}
	}